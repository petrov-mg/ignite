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

package org.apache.ignite.internal.processors.tracing;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.SpanStatus;
import org.apache.ignite.spi.tracing.SpiSpecificSpan;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.util.GridClientByteUtils.intToBytes;
import static org.apache.ignite.internal.util.GridClientByteUtils.shortToBytes;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/**
 * Implementation of a {@link Span}
 */
public class SpanImpl implements Span {
    /** Spi specific span delegate. */
    private final SpiSpecificSpan spiSpecificSpan;

    /** Span type. */
    private final SpanType spanType;

    /** Set of extra included scopes for given span in addition to span's scope that is supported by default. */
    private final Set<Scope> includedScopes;

    /**
     * Constructor
     *
     * @param spiSpecificSpan Spi specific span.
     * @param spanType Type of a span.
     * @param includedScopes Set of included scopes.
     */
    public SpanImpl(
        SpiSpecificSpan spiSpecificSpan,
        SpanType spanType,
        Set<Scope> includedScopes) {
        this.spiSpecificSpan = spiSpecificSpan;
        this.spanType = spanType;
        this.includedScopes = includedScopes;
    }

    @Override public Span addTag(String tagName, Supplier<String> tagValSupplier) {
        spiSpecificSpan.addTag(tagName, tagValSupplier.get());

        return this;
    }

    @Override public Span addLog(Supplier<String> logDescSupplier) {
        spiSpecificSpan.addLog(logDescSupplier.get());

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span setStatus(SpanStatus spanStatus) {
        spiSpecificSpan.setStatus(spanStatus);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span end() {
        spiSpecificSpan.end();

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnded() {
        return spiSpecificSpan.isEnded();
    }

    /** {@inheritDoc} */
    @Override public SpanType type() {
        return spanType;
    }

    /** {@inheritDoc} */
    @Override public Set<Scope> includedScopes() {
        return includedScopes;
    }

    /** {@inheritDoc} */
    @Override public Span createChildSpan(double samplingRate, SpanType spanTypeToCreate) {
        if (!isChainable(spanTypeToCreate.scope()))
            return NoopSpan.INSTANCE;

        Set<Scope> mergedIncludedScopes = new HashSet<>(includedScopes());

        mergedIncludedScopes.add(type().scope());
        mergedIncludedScopes.remove(spanTypeToCreate.scope());

        return new SpanImpl(
            spiSpecificSpan.createChildSpan(spanTypeToCreate.spanName(), samplingRate),
            spanTypeToCreate,
            mergedIncludedScopes);
    }

    @Override public byte[] toByteArray() {
        // Spi specific serialized span.
        byte[] spiSpecificSerializedSpan = spiSpecificSpan.toByteArray()

        int serializedSpanLen = SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
            INCLUDED_SCOPES_SIZE_BYTE_LENGTH + spiSpecificSerializedSpan.length + SCOPE_INDEX_BYTE_LENGTH *
            span.includedScopes().size();

        byte[] serializedSpanBytes = new byte[serializedSpanLen];

        // Skip special flags bytes.

        // Spi type idx.
        serializedSpanBytes[SPI_TYPE_OFF] = getSpi().type().index();

        // Major protocol version;
        serializedSpanBytes[MAJOR_PROTOCOL_VERSION_OFF] = MAJOR_PROTOCOL_VERSION;

        // Minor protocol version;
        serializedSpanBytes[MINOR_PROTOCOL_VERSION_OFF] = MINOR_PROTOCOL_VERSION;

        // Spi specific serialized span length.
        System.arraycopy(
            intToBytes(spiSpecificSerializedSpan.length),
            0,
            serializedSpanBytes,
            SPI_SPECIFIC_SERIALIZED_SPAN_BYTES_LENGTH_OFF,
            SPI_SPECIFIC_SERIALIZED_SPAN_BYTES_LENGTH);

        // Spi specific span.
        System.arraycopy(
            spiSpecificSerializedSpan,
            0,
            serializedSpanBytes,
            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF,
            spiSpecificSerializedSpan.length);

        // Span type.
        System.arraycopy(
            intToBytes(span.type().index()),
            0,
            serializedSpanBytes,
            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + spiSpecificSerializedSpan.length,
            PARENT_SPAN_TYPE_BYTES_LENGTH );

        assert span.includedScopes() != null;

        // Included scope size
        System.arraycopy(
            intToBytes(span.includedScopes().size()),
            0,
            serializedSpanBytes,
            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
                spiSpecificSerializedSpan.length,
            INCLUDED_SCOPES_SIZE_BYTE_LENGTH);

        int includedScopesCnt = 0;

        if (!span.includedScopes().isEmpty()) {
            for (Scope includedScope : span.includedScopes()) {
                System.arraycopy(
                    shortToBytes(includedScope.idx()),
                    0,
                    serializedSpanBytes,
                    SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
                        INCLUDED_SCOPES_SIZE_BYTE_LENGTH + spiSpecificSerializedSpan.length +
                        SCOPE_INDEX_BYTE_LENGTH * includedScopesCnt++,
                    SCOPE_INDEX_BYTE_LENGTH);
            }
        }

        return serializedSpanBytes;
    }

    /**
     * @return Spi specific span delegate.
     */
    public SpiSpecificSpan spiSpecificSpan() {
        return spiSpecificSpan;
    }
}
