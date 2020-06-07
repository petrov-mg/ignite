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

package org.apache.ignite.spi.tracing.opencensus;

import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.BlankSpan;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.export.SpanExporter;
import io.opencensus.trace.samplers.Samplers;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.opencensus.spi.tracing.OpenCensusTraceExporter;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.tracing.SpanStatus;
import org.apache.ignite.spi.tracing.SpiSpecificSpan;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.TracingSpiType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_NEVER;

/**
 * Tracing SPI implementation based on OpenCensus library.
 *
 * If you have OpenCensus Tracing in your environment use the following code for configuration:
 * <code>
 *     IgniteConfiguration cfg;
 *
 *     cfg.setTracingSpi(new OpenCensusTracingSpi());
 * </code>
 * If you don't have OpenCensus Tracing:
 * <code>
 *     IgniteConfiguration cfg;
 *
 *     cfg.setTracingSpi(new OpenCensusTracingSpi(new ZipkinExporterHandler(...)));
 * </code>
 *
 * See constructors description for detailed explanation.
 */
@IgniteSpiMultipleInstancesSupport(value = true)
@IgniteSpiConsistencyChecked(optional = true)
public class OpenCensusTracingSpi extends IgniteSpiAdapter implements TracingSpi {
    /** Configured exporters. */
    private final List<OpenCensusTraceExporter> exporters;

    /** Flag indicates that external Tracing is used in environment. In this case no exporters will be started. */
    private final boolean externalProvider;

    /**
     * This constructor is used if environment (JVM) already has OpenCensus tracing.
     * In this case traces from the node will go trough externally registered exporters by an user himself.
     *
     * @see Tracing#getExportComponent()
     */
    public OpenCensusTracingSpi() {
        exporters = null;

        externalProvider = true;
    }

    /**
     * This constructor is used if environment (JVM) hasn't OpenCensus tracing.
     * In this case provided exporters will start and traces from the node will go through it.
     *
     * @param exporters Exporters.
     */
    public OpenCensusTracingSpi(SpanExporter.Handler... exporters) {
        this.exporters = Arrays.stream(exporters).map(OpenCensusTraceExporter::new).collect(Collectors.toList());

        externalProvider = false;
    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan create(@NotNull String name, double samplingRate) {
        try {
            return new OpenCensusSpanAdapter(
                Tracing.getTracer()
                    .spanBuilderWithExplicitParent(name, null)
                    .setSampler(toSampler(samplingRate))
                    .startSpan()
            );
        }
        catch (Exception e) {
            LT.warn(log, "Failed to create span from parent " +
                "[spanName=" + name + ']');

            return new OpenCensusSpanAdapter(BlankSpan.INSTANCE);
        }
    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan create(@NotNull String name, double samplingRate, @Nullable byte[] parentSerializedSpan) throws Exception {
        return new OpenCensusSpanAdapter(
            Tracing.getTracer()
                .spanBuilderWithRemoteParent(
                    name,
                    Tracing.getPropagationComponent().getBinaryFormat().fromByteArray(parentSerializedSpan))
                .setSampler(toSampler(samplingRate))
                .startSpan()
        );
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return "OpenCensusTracingSpi";
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        if (!externalProvider && exporters != null)
            for (OpenCensusTraceExporter exporter : exporters)
                exporter.start(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (!externalProvider && exporters != null)
            for (OpenCensusTraceExporter exporter : exporters)
                exporter.stop();
    }

    /** {@inheritDoc} */
    @Override public TracingSpiType type() {
        return TracingSpiType.OPEN_CENSUS_TRACING_SPI;
    }

    /** */
    private static  Sampler toSampler(double samplingRate) {
        if (Double.compare(samplingRate, SAMPLING_RATE_NEVER) == 0) {
            // We should never get here, because of an optimization that produces {@code NoopSpan.Instance}
            // instead of a span with {@code SAMPLING_RATE_NEVER} sampling rate. It is useful cause in case
            // of {@code NoopSpan.Instance} we will not send span data over the network.assert false;

           return Samplers.neverSample(); // Just in case.
        }
        else if (Double.compare(samplingRate, SAMPLING_RATE_ALWAYS) == 0)
            return Samplers.alwaysSample();
        else
            return Samplers.probabilitySampler(samplingRate);
    }

    /** */
    private static class OpenCensusSpanAdapter implements SpiSpecificSpan {
        /** OpenCensus span delegate. */
        private final Span span;

        /** Flag indicates that span is ended. */
        private volatile boolean ended;

        /**
         * @param span OpenCensus span delegate.
         */
        private OpenCensusSpanAdapter(Span span) {
            this.span = span;
        }

        /** {@inheritDoc} */
        @Override public OpenCensusSpanAdapter addTag(String tagName, String tagVal) {
            tagVal = tagVal != null ? tagVal : "null";

            span.putAttribute(tagName, AttributeValue.stringAttributeValue(tagVal));

            return this;
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan addTag(String tagName, long tagVal) {
            span.putAttribute(tagName, AttributeValue.longAttributeValue(tagVal));

            return this;
        }

        /** {@inheritDoc} */
        @Override public OpenCensusSpanAdapter addLog(String logDesc) {
            span.addAnnotation(logDesc);

            return this;
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan addLog(String logDesc, Map<String, String> attrs) {
            span.addAnnotation(Annotation.fromDescriptionAndAttributes(
                logDesc,
                attrs.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> AttributeValue.stringAttributeValue(e.getValue())
                    ))
            ));

            return this;
        }

        /** {@inheritDoc} */
        @Override public OpenCensusSpanAdapter setStatus(SpanStatus spanStatus) {
            span.setStatus(StatusMatchTable.match(spanStatus));

            return this;
        }

        /** {@inheritDoc} */
        @Override public OpenCensusSpanAdapter end() {
            span.end();

            ended = true;

            return this;
        }

        /** {@inheritDoc} */
        @Override public SpiSpecificSpan createChildSpan(@NotNull String name, double samplingRate) {
            try {
                return new OpenCensusSpanAdapter(
                    Tracing.getTracer()
                        .spanBuilderWithExplicitParent(name, span)
                        .setSampler(toSampler(samplingRate))
                        .startSpan()
                );
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to create span from parent " +
                    "[spanName=" + name + ", parentSpan=" + span + "]", e);
            }
        }

        /** {@inheritDoc} */
        @Override public byte[] toByteArray() {
            return Tracing.getPropagationComponent()
                .getBinaryFormat()
                .toByteArray(span.getContext());
        }

        /** {@inheritDoc} */
        @Override public boolean isEnded() {
            return ended;
        }
    }
}
