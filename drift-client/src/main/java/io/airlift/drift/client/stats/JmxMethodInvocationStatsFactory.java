/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.client.stats;

import io.airlift.drift.client.ThriftServiceMetadata;
import io.airlift.drift.transport.MethodMetadata;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNameBuilder;

import javax.inject.Inject;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

public class JmxMethodInvocationStatsFactory
        implements Closeable, MethodInvocationStatsFactory
{
    private final MBeanExporter exporter;
    private final ConcurrentMap<String, MethodInvocationStat> stats = new ConcurrentHashMap<>();

    @Inject
    public JmxMethodInvocationStatsFactory(MBeanExporter exporter)
    {
        this.exporter = requireNonNull(exporter, "exporter is null");
    }

    @Override
    public synchronized MethodInvocationStat getStat(ThriftServiceMetadata serviceMetadata, MethodMetadata metadata)
    {
        String objectName = new ObjectNameBuilder("thrift.client")
                .withProperty("name", serviceMetadata.getName())
                .withProperty("method", metadata.getName())
                .build();

        MethodInvocationStat stat = stats.get(objectName);
        if (stat == null) {
            stat = new JmxMethodInvocationStat(metadata.getName());
            exporter.export(objectName, stat);
        }

        return stat;
    }

    @Override
    public synchronized void close()
    {
        for (String name : stats.keySet()) {
            exporter.unexport(name);
        }
        stats.clear();
    }
}
