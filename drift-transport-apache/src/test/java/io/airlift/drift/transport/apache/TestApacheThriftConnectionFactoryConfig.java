/*
 * Copyright (C) 2017 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.drift.transport.apache;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.drift.transport.apache.client.ApacheThriftConnectionFactoryConfig;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestApacheThriftConnectionFactoryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ApacheThriftConnectionFactoryConfig.class)
                .setThreadCount(null)
                .setSocksProxy(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("thrift.client.thread-count", "99")
                .put("thrift.client.socks-proxy", "example.com:9876")
                .build();

        ApacheThriftConnectionFactoryConfig expected = new ApacheThriftConnectionFactoryConfig()
                .setThreadCount(99)
                .setSocksProxy(HostAndPort.fromParts("example.com", 9876));

        assertFullMapping(properties, expected);
    }
}
