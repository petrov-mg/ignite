/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.profiling;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer.BufferMode;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

/**
 * Profiling implementation based on logging statistics to a profiling file.
 * <p>
 * Each node collects statistics to a profiling file placed under {@link #PROFILING_DIR}.
 * <p>
 * To build the performance report follow:
 * <ol>
 *     <li>Start profiling. See {@link #startProfiling(long, int, int)}</li>
 *     <li>Collect workload statistics.</li>
 *     <li>Stop profiling. See {@link #stopProfiling()}</li>
 *     <li>Collect profiling files from all nodes under an empty directory.</li>
 *     <li>Run script {@code ./bin/profiling.sh path_to_files} to build the performance report.</li>
 * </ol>
 * <b>Note:</b> Start profiling again will erase previous profiling files.
 */
public class LogFileProfiling implements IgniteProfiling {
    /** Default Maximum file size in bytes. Profiling will be stopped when the size exceeded. */
    public static final long DFLT_FILE_MAX_SIZE = 16 * 1024 * 1024 * 1024L;

    /** Default off heap buffer size in bytes. */
    public static final int DFLT_BUFFER_SIZE = 32 * 1024 * 1024;

    /** Default minimal batch size to flush in bytes. */
    public static final int DFLT_FLUSH_SIZE = 8 * 1024 * 1024;

    /** Directory to store profiling files. Placed under Ignite work directory. */
    public static final String PROFILING_DIR = "profiling";

    /** Factory to provide I/O interface for profiling file. */
    private final FileIOFactory fileIoFactory = new RandomAccessFileIOFactory();

    /** Profiling enabled flag. */
    private volatile boolean enabled;

    /** Profiling file writer. */
    @Nullable private volatile FileWriter fileWriter;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** @param ctx Kernal context. */
    public LogFileProfiling(GridKernalContext ctx) {
        log = ctx.log(getClass());

        this.ctx = ctx;
    }

    /** @return {@code True} if profiling enabled. */
    public boolean profilingEnabled() {
        return enabled;
    }

    /**
     * Starts profiling.
     *
     * @param maxFileSize Maximum file size in bytes.
     * @param bufferSize Off heap buffer size in bytes.
     * @param flushBatchSize Minimal batch size to flush in bytes.
     */
    public synchronized void startProfiling(long maxFileSize, int bufferSize, int flushBatchSize) {
        if (enabled)
            return;

        FileWriter writer = fileWriter;

        // Profiling is stopping.
        if (writer != null) {
            try {
                writer.shutdown().get();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to wait for previous profiling stopping.", e);
            }
        }

        assert fileWriter == null;

        try {
            File file = profilingFile(ctx);

            U.delete(file);

            FileIO fileIo = fileIoFactory.create(file);

            fileIo.position(0);

            fileWriter = new FileWriter(ctx, fileIo, maxFileSize, bufferSize, flushBatchSize, log);

            new IgniteThread(fileWriter).start();

            enabled = true;

            log.info("Profiling started [file=" + file.getAbsolutePath() + ']');
        }
        catch (IOException | IgniteCheckedException e) {
            log.error("Failed to start profiling.", e);

            throw new IgniteException("Failed to start profiling.", e);
        }

        profilingStart(ctx.localNodeId(), ctx.igniteInstanceName(), IgniteVersionUtils.VER_STR, U.currentTimeMillis());
    }

    /** Stops profiling. */
    public IgniteInternalFuture<Void> stopProfiling() {
        synchronized (this) {
            if (!enabled)
                return new GridFinishedFuture<>();

            enabled = false;
        }

        log.info("Stopping profiling.");

        FileWriter fileWriter = this.fileWriter;

        if (fileWriter != null)
            return fileWriter.shutdown();

        return new GridFinishedFuture<>();
    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(CacheOperationType type, int cacheId, long startTime, long duration) {
        int size = /*type*/ 1 +
            /*cacheId*/ 4 +
            /*startTime*/ 8 +
            /*duration*/ 8;

        SegmentedRingByteBuffer.WriteSegment seg = reserveBuffer(OperationType.CACHE_OPERATION, size);

        if (seg == null)
            return;

        ByteBuffer buf = seg.buffer();

        buf.put((byte)type.ordinal());
        buf.putInt(cacheId);
        buf.putLong(startTime);
        buf.putLong(duration);

        seg.release();
    }

    /** {@inheritDoc} */
    @Override public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commit) {
        int size = /*cacheIds*/ 4 + cacheIds.size() * 4 +
            /*startTime*/ 8 +
            /*duration*/ 8 +
            /*commit*/ 1;

        SegmentedRingByteBuffer.WriteSegment seg = reserveBuffer(OperationType.TRANSACTION, size);

        if (seg == null)
            return;

        ByteBuffer buf = seg.buffer();

        buf.putInt(cacheIds.size());

        GridIntIterator iter = cacheIds.iterator();

        while (iter.hasNext())
            buf.putInt(iter.next());

        buf.putLong(startTime);
        buf.putLong(duration);
        buf.put(commit ? (byte)1 : 0);

        seg.release();
    }

    /** {@inheritDoc} */
    @Override public void query(GridCacheQueryType type, String text, long id, long startTime, long duration,
        boolean success) {
        FileWriter writer = fileWriter;

        if (writer == null)
            return;

        Short strId = writer.stringId(text);

        boolean needWriteStr = strId == null;

        byte[] strBytes = null;

        int size = /*type*/ 1 +
            /*compactStringFlag*/ 1 +
            /*strId*/ 2 +
            /*id*/ 8 +
            /*startTime*/ 8 +
            /*duration*/ 8 +
            /*success*/ 1;

        if (needWriteStr) {
            strBytes = text.getBytes();

            size += /*text*/ 4 + strBytes.length;

            strId = writer.generateStringId(text);
        }

        SegmentedRingByteBuffer.WriteSegment seg = reserveBuffer(OperationType.QUERY, size);

        if (seg == null)
            return;

        ByteBuffer buf = seg.buffer();

        buf.put((byte)type.ordinal());
        buf.put(needWriteStr ? (byte)1 : 0);
        buf.putShort(strId);

        if (needWriteStr) {
            buf.putInt(strBytes.length);
            buf.put(strBytes);
        }

        buf.putLong(id);
        buf.putLong(startTime);
        buf.putLong(duration);
        buf.put(success ? (byte)1 : 0);

        seg.release();
    }

    /** {@inheritDoc} */
    @Override public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads,
        long physicalReads) {
        int size = /*type*/ 1 +
            /*queryNodeId*/ 16 +
            /*id*/ 8 +
            /*logicalReads*/ 8 +
            /*physicalReads*/ 8;

        SegmentedRingByteBuffer.WriteSegment seg = reserveBuffer(OperationType.QUERY_READS, size);

        if (seg == null)
            return;

        ByteBuffer buf = seg.buffer();

        buf.put((byte)type.ordinal());
        writeUuid(buf, queryNodeId);
        buf.putLong(id);
        buf.putLong(logicalReads);
        buf.putLong(physicalReads);

        seg.release();
    }

    /** {@inheritDoc} */
    @Override public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        FileWriter writer = fileWriter;

        if (writer == null)
            return;

        Short strId = writer.stringId(taskName);

        boolean needWriteStr = strId == null;

        byte[] strBytes = null;

        int size = /*sesId*/ 24 +
            /*compactStringFlag*/ 1 +
            /*strId*/ 2 +
            /*startTime*/ 8 +
            /*duration*/ 8 +
            /*affPartId*/ 4;

        if (needWriteStr) {
            strBytes = taskName.getBytes();

            size += /*taskName*/ 4 + strBytes.length;

            strId = writer.generateStringId(taskName);
        }

        SegmentedRingByteBuffer.WriteSegment seg = reserveBuffer(OperationType.TASK, size);

        if (seg == null)
            return;

        ByteBuffer buf = seg.buffer();

        writeIgniteUuid(buf, sesId);
        buf.put(needWriteStr ? (byte)1 : 0);
        buf.putShort(strId);

        if (needWriteStr) {
            buf.putInt(strBytes.length);
            buf.put(strBytes);
        }

        buf.putLong(startTime);
        buf.putLong(duration);
        buf.putInt(affPartId);

        seg.release();
    }

    /** {@inheritDoc} */
    @Override public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        int size = /*sesId*/ 24 +
            /*queuedTime*/ 8 +
            /*startTime*/ 8 +
            /*duration*/ 8 +
            /*timedOut*/ 1;

        SegmentedRingByteBuffer.WriteSegment seg = reserveBuffer(OperationType.JOB, size);

        if (seg == null)
            return;

        ByteBuffer buf = seg.buffer();

        writeIgniteUuid(buf, sesId);
        buf.putLong(queuedTime);
        buf.putLong(startTime);
        buf.putLong(duration);
        buf.put(timedOut ? (byte)1 : 0);

        seg.release();
    }

    /** {@inheritDoc} */
    @Override public void cacheStart(int cacheId, long startTime, String cacheName, String groupName,
        boolean userCache) {
        byte[] cacheNameBytes = cacheName.getBytes();
        byte[] groupNameBytes = groupName == null ? new byte[0] : groupName.getBytes();

        int size = /*cacheId*/ 4 +
            /*startTime*/ 8 +
            /*cacheName*/ 4 + cacheNameBytes.length +
            /*groupName*/ 4 + groupNameBytes.length +
            /*userCacheFlag*/ 1;

        SegmentedRingByteBuffer.WriteSegment seg = reserveBuffer(OperationType.CACHE_START, size);

        if (seg == null)
            return;

        ByteBuffer buf = seg.buffer();

        buf.putInt(cacheId);
        buf.putLong(startTime);

        buf.putInt(cacheNameBytes.length);
        buf.put(cacheNameBytes);

        if (groupNameBytes == null)
            buf.putInt(0);
        else {
            buf.putInt(groupNameBytes.length);
            buf.put(groupNameBytes);
        }

        buf.put(userCache ? (byte)1 : 0);

        seg.release();
    }

    /** {@inheritDoc} */
    @Override public void profilingStart(UUID nodeId, String igniteInstanceName, String igniteVersion, long startTime) {
        byte[] nameBytes = igniteInstanceName.getBytes();
        byte[] verBytes = igniteVersion.getBytes();

        int size = /*nodeId*/ 16 +
            /*igniteInstanceName*/ 4 + nameBytes.length +
            /*version*/ 4 + verBytes.length +
            /*profilingStartTime*/ 8;

        SegmentedRingByteBuffer.WriteSegment seg = reserveBuffer(OperationType.PROFILING_START, size);

        if (seg == null)
            return;

        ByteBuffer buf = seg.buffer();

        writeUuid(buf, nodeId);
        buf.putInt(nameBytes.length);
        buf.put(nameBytes);
        buf.putInt(verBytes.length);
        buf.put(verBytes);
        buf.putLong(startTime);

        seg.release();
    }

    /**
     * Reserves buffer's write segment.
     *
     * @return Buffer's write segment or {@code null} if not enought space or profiling stopping.
     */
    private SegmentedRingByteBuffer.WriteSegment reserveBuffer(OperationType type, int size) {
        FileWriter fileWriter = this.fileWriter;

        // Profiling stopping.
        if (fileWriter == null)
            return null;

        SegmentedRingByteBuffer.WriteSegment seg = fileWriter.writeSegment(size + /*type*/ 1);

        if (seg == null) {
            LT.warn(log, "The profiling buffer size is too small. Some operations will not be profiled.");

            return null;
        }

        // Ring buffer closed (profiling stopping) or maximum size reached.
        if (seg.buffer() == null) {
            seg.release();

            if (!fileWriter.isCancelled()) {
                log.warning("The profiling file maximum size is reached. Profiling will be stopped.");

                // TODO Stop on all nodes.
                stopProfiling();
            }

            return null;
        }

        ByteBuffer buf = seg.buffer();

        buf.put((byte)type.ordinal());

        return seg;
    }

    /** @return Profiling file. */
    public static File profilingFile(GridKernalContext ctx) throws IgniteCheckedException {
        String igniteWorkDir = U.workDirectory(ctx.config().getWorkDirectory(), ctx.config().getIgniteHome());

        File profilingDir = U.resolveWorkDirectory(igniteWorkDir, PROFILING_DIR, false);

        return new File(profilingDir, "node-" + ctx.localNodeId() + ".prf");
    }

    /** Writes {@link UUID} to buffer. */
    public static void writeUuid(ByteBuffer buf, UUID uuid) {
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
    }

    /** Reads {@link UUID} from buffer. */
    public static UUID readUuid(ByteBuffer buf) {
        return new UUID(buf.getLong(), buf.getLong());
    }

    /** Writes {@link IgniteUuid} to buffer. */
    public static void writeIgniteUuid(ByteBuffer buf, IgniteUuid uuid) {
        buf.putLong(uuid.globalId().getMostSignificantBits());
        buf.putLong(uuid.globalId().getLeastSignificantBits());
        buf.putLong(uuid.localId());
    }

    /** Reads {@link IgniteUuid} from buffer. */
    public static IgniteUuid readIgniteUuid(ByteBuffer buf) {
        UUID globalId = new UUID(buf.getLong(), buf.getLong());

        return new IgniteUuid(globalId, buf.getLong());
    }

    /** Worker to write to profiling file. */
    private class FileWriter extends GridWorker {
        /** Maximum cached string count. */
        private static final short MAX_CACHED_STRING_COUNT = Short.MAX_VALUE;

        /** Profiling file I/O. */
        private final FileIO fileIo;

        /** File write buffer. */
        private final SegmentedRingByteBuffer ringByteBuffer;

        /** Minimal batch size to flush in bytes. */
        private final int flushBatchSize;

        /** Size of ready for flushing bytes. */
        private final AtomicInteger readyForFlushSize = new AtomicInteger();

        /** Stop file writer future. */
        GridFutureAdapter<Void> stopFut = new GridFutureAdapter<>();

        /** Cached strings by id. */
        private final ConcurrentHashMap<String, Short> stringIds = new ConcurrentHashMap<>();

        /** String id generator. */
        private final AtomicInteger idsGen = new AtomicInteger();

        /**
         * @param ctx Kernal context.
         * @param fileIo Profiling file I/O.
         * @param maxFileSize Maximum file size in bytes.
         * @param bufferSize Off heap buffer size in bytes.
         * @param flushBatchSize Minimal batch size to flush in bytes.
         * @param log Logger.
         */
        FileWriter(GridKernalContext ctx, FileIO fileIo, long maxFileSize, int bufferSize, int flushBatchSize,
            IgniteLogger log) {
            super(ctx.igniteInstanceName(), "profiling-writer%" + ctx.igniteInstanceName(), log);

            this.fileIo = fileIo;
            this.flushBatchSize = flushBatchSize;

            ringByteBuffer = new SegmentedRingByteBuffer(bufferSize, maxFileSize, BufferMode.DIRECT);

            ringByteBuffer.init(0);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled() && !Thread.interrupted()) {
                blockingSectionBegin();

                try {
                    synchronized (this) {
                        while (readyForFlushSize.get() < flushBatchSize && !isCancelled())
                            wait();
                    }

                    flushBuffer();
                }
                finally {
                    blockingSectionEnd();
                }
            }

            fileWriter = null;

            ringByteBuffer.close();

            // Make sure that all producers released their buffers to safe deallocate memory.
            ringByteBuffer.poll();

            ringByteBuffer.free();

            U.closeQuiet(fileIo);

            stringIds.clear();

            stopFut.onDone();

            log.info("Profiling stopped.");
        }

        /** @return Unique per file string identifier. {@code Null} if there is no cached identifier. */
        Short stringId(String str) {
            return stringIds.get(str);
        }

        /** @return Generate unique per file string identifier. {@code -1} if max cached limit exceeded. */
        short generateStringId(String str) {
            if (idsGen.get() > MAX_CACHED_STRING_COUNT)
                return -1;

            return stringIds.computeIfAbsent(str,
                s -> (short)idsGen.updateAndGet(id -> id < MAX_CACHED_STRING_COUNT ? id + 1 : -1));
        }

        /** @return Write segment.*/
        SegmentedRingByteBuffer.WriteSegment writeSegment(int size) {
            SegmentedRingByteBuffer.WriteSegment seg = ringByteBuffer.offer(size);

            if (seg != null) {
                int readySize = readyForFlushSize.addAndGet(size);

                if (readySize >= DFLT_FLUSH_SIZE) {
                    synchronized (this) {
                        notify();
                    }
                }
            }

            return seg;
        }

        /** Flushes to disk available bytes from the ring buffer. */
        private void flushBuffer() {
            List<SegmentedRingByteBuffer.ReadSegment> segs = ringByteBuffer.poll();

            if (segs == null)
                return;

            try {
                for (int i = 0; i < segs.size(); i++) {
                    SegmentedRingByteBuffer.ReadSegment seg = segs.get(i);

                    try {
                        readyForFlushSize.addAndGet(-seg.buffer().remaining());

                        fileIo.writeFully(seg.buffer());
                    }
                    finally {
                        seg.release();
                    }
                }

                fileIo.force();
            } catch (IOException e) {
                log.error("Unable to write to file. Profiling will be stopped.", e);

                stopProfiling();
            }
        }

        /** Shutted down the worker. */
        private IgniteInternalFuture<Void> shutdown() {
            isCancelled = true;

            synchronized (this) {
                notify();
            }

            return stopFut;
        }
    }

    /** Operation type. */
    public enum OperationType {
        /** Cache operation. */
        CACHE_OPERATION,

        /** Transaction. */
        TRANSACTION,

        /** Query. */
        QUERY,

        /** Query reads. */
        QUERY_READS,

        /** Task. */
        TASK,

        /** Job. */
        JOB,

        /** Cache start. */
        CACHE_START,

        /** Profiling start. */
        PROFILING_START;

        /** Values. */
        private static final OperationType[] VALS = values();

        /** @return Operation type from ordinal. */
        public static OperationType fromOrdinal(byte ord) {
            return ord < 0 || ord >= VALS.length ? null : VALS[ord];
        }
    }
}
