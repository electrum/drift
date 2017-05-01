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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransport;

import java.util.List;

import static java.util.Objects.requireNonNull;

class ThriftUnframedDecoder
        extends ByteToMessageDecoder
{
    private final TProtocolFactory protocolFactory;
    private final int maxFrameSize;

    public ThriftUnframedDecoder(TProtocolFactory protocolFactory, int maxFrameSize)
    {
        this.protocolFactory = requireNonNull(protocolFactory, "protocolFactory is null");
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    protected final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    {
        Object decoded = decode(ctx, in);
        if (decoded != null) {
            out.add(decoded);
        }
    }

    private Object decode(ChannelHandlerContext ctx, ByteBuf buffer)
    {
        int frameOffset = buffer.readerIndex();
        try {
            TTransport transport = new TChannelBufferInputTransport(buffer);
            TProtocol protocol = protocolFactory.getProtocol(transport);

            protocol.readMessageBegin();
            TProtocolUtil.skip(protocol, TType.STRUCT);
            protocol.readMessageEnd();

            int frameLength = buffer.readerIndex() - frameOffset;
            if (frameLength > maxFrameSize) {
                ctx.fireExceptionCaught(new TooLongFrameException("Response message exceeds max size " + maxFrameSize + ": " + frameLength + " - discarded"));
            }

            return buffer.slice(frameOffset, frameLength).retain();
        }
        catch (Throwable th) {
            buffer.readerIndex(frameOffset);
            return null;
        }
    }
}
