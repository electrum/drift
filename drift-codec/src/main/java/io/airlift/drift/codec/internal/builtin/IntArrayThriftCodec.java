/*
 * Copyright (C) 2014 Facebook, Inc.
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
package io.airlift.drift.codec.internal.builtin;

import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.internal.TProtocolReader;
import io.airlift.drift.codec.internal.TProtocolWriter;
import io.airlift.drift.codec.metadata.ThriftType;
import org.apache.thrift.protocol.TProtocol;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class IntArrayThriftCodec
        implements ThriftCodec<int[]>
{
    @Override
    public ThriftType getType()
    {
        return ThriftType.array(ThriftType.I32);
    }

    @Override
    public int[] read(TProtocol protocol)
            throws Exception
    {
        checkNotNull(protocol, "protocol is null");
        return new TProtocolReader(protocol).readI32Array();
    }

    @Override
    public void write(int[] value, TProtocol protocol)
            throws Exception
    {
        checkNotNull(value, "value is null");
        checkNotNull(protocol, "protocol is null");
        new TProtocolWriter(protocol).writeI32Array(value);
    }
}