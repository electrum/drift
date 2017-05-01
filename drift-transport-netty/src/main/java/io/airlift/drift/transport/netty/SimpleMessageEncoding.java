/*
 * Copyright (C) 2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.airlift.drift.transport.netty;

import io.airlift.drift.transport.MethodMetadata;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class SimpleMessageEncoding
        implements MessageEncoding
{
    private final TProtocolFactory protocolFactory;

    public SimpleMessageEncoding(TProtocolFactory protocolFactory)
    {
        this.protocolFactory = requireNonNull(protocolFactory, "protocolFactory is null");
    }

    @Override
    public ByteBuf writeRequest(ByteBufAllocator allocator, int sequenceId, MethodMetadata method, List<Object> parameters, Map<String, String> headers)
            throws Exception
    {
        checkArgument(headers.isEmpty(), "Headers are only supported with header transport");
        return MessageEncoding.encodeRequest(protocolFactory, allocator, sequenceId, method, parameters);
    }

    @Override
    public OptionalInt extractResponseSequenceId(ByteBuf buffer)
    {
        try {
            TTransport inputTransport = new TChannelBufferInputTransport(buffer.duplicate());
            TMessage message = protocolFactory.getProtocol(inputTransport).readMessageBegin();
            return OptionalInt.of(message.seqid);
        }
        catch (Throwable ignored) {
        }
        return OptionalInt.empty();
    }

    @Override
    public Object readResponse(ByteBuf buffer, int sequenceId, MethodMetadata method)
            throws Exception
    {
        return MessageEncoding.decodeResponse(protocolFactory, buffer, sequenceId, method);
    }
}
