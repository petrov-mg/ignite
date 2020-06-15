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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanImpl;
import org.apache.ignite.spi.tracing.NoopSpiSpecificSpan;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.apache.ignite.internal.processors.monitoring.opencensus.SpanAttachmentSelfTest.TestAttachment.cntr1;
import static org.apache.ignite.internal.processors.monitoring.opencensus.SpanAttachmentSelfTest.TestAttachment.cntr2;
import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_NODE_FAILED;

/** */
public class SpanAttachmentSelfTest {
    /** */
    @Test
    public void test() throws Exception {
        Span span = new SpanImpl(NoopSpiSpecificSpan.INSTANCE, DISCOVERY_NODE_FAILED, Collections.emptySet());

        try {
            span.attachment(TestAttachment::invoke);

            fail();
        }
        catch (Exception ignored) {
           // No-op.
        }

        span.attach(TestAttachment::create);

        span.attachment(TestAttachment::invoke);

        Span noopSpan = NoopSpan.INSTANCE;

        noopSpan.attachment(TestAttachment::invoke);

        noopSpan.attach(TestAttachment::create);

        noopSpan.attachment(TestAttachment::invoke);

        assertEquals(1, cntr2.get());
        assertEquals(1, cntr1.get());
    }

    /** */
    public static class TestAttachment {
        public static AtomicInteger cntr1 = new AtomicInteger();

        public static AtomicInteger cntr2 = new AtomicInteger();
        /**
         *
         */
        public static TestAttachment create() {
            cntr1.incrementAndGet();

            return new TestAttachment();
        }

        /**
         *
         */
        public void invoke() {
            cntr2.incrementAndGet();
        }

    }
}
