package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Arrays;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class MultithreadedPageMemoryWarmingUpTest extends PageMemoryWarmingUpTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private IgniteConfiguration getMultithreadedWarmUpCfg(String igniteInstanceName, int warmingUpThreads,
        int dumpProcThreads) throws Exception {
        IgniteConfiguration cfg = getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setPageSize(4 * 1024)
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(1024 * 1024 * 1024)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(maxMemorySize)
                .setInitialSize(maxMemorySize)
                .setPersistenceEnabled(true)
                .setWarmingUpEnabled(true)
                .setWaitWarmingUpOnStart(waitWarmingUpOnStart)
                .setWarmingUpRuntimeDumpDelay(warmingUpRuntimeDumpDelay)
                .setWarmingUpMultithreadedEnabled(true)
                .setWarmingUpThreads(warmingUpThreads)
                .setDumpProcThreads(dumpProcThreads)
            );

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** */
    public void warmUpMultithreaded(String igniteInstanceName, int warmingUpThreads, int dumpProcThreads)
        throws Exception {
        IgniteEx ignite = startGrid(
            getMultithreadedWarmUpCfg(igniteInstanceName, warmingUpThreads, dumpProcThreads));

        ignite.cluster().active(true);

        IgniteCache<Integer, int[]> cache = ignite.getOrCreateCache(CACHE_NAME);

        int[] val = new int[valSize];

        for (int i = 0; i < valCnt; i++) {
            Arrays.fill(val, i);

            cache.put(i, val);
        }

        forceCheckpoint(ignite);

        ignite.close();

        pushOutDiskCache();

        ignite = startGrid(
            getMultithreadedWarmUpCfg(igniteInstanceName, warmingUpThreads, dumpProcThreads));

        ignite.cluster().active(true);

        cache = ignite.getOrCreateCache(CACHE_NAME);

        for (int i = valCnt; i >= 0; i--) {
            long startTs = U.currentTimeMillis();

            int key = i % valCnt; // Key '0' supposed as cold.

            val = cache.get(key);

            System.out.println("### " + key + " get in " + (U.currentTimeMillis() - startTs) + " ms, val=" +
                (val != null ? val.getClass().getSimpleName() + " [" + val.length + "]" : null));
        }
    }

    /** */
    @Test
    public void testMultithreadedWarmUp() throws Exception {
        for (int i = 0; i < 3; i++) {
            for (int warmingUpThreads = 56; warmingUpThreads <= 56; warmingUpThreads *= 2) {
                for (int dumpProcThreads = 2; dumpProcThreads <= 8; dumpProcThreads *= 2) {
                    beforeTest();

                    warmUpMultithreaded("warmingUpThread-" + warmingUpThreads +
                        "-dumpProcThreads-" + dumpProcThreads, warmingUpThreads, dumpProcThreads);

                    afterTest();
                }
            }
        }
    }
}
