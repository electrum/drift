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

import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.transport.netty.DriftNettyClientConfig.Protocol.BINARY;
import static io.airlift.drift.transport.netty.DriftNettyClientConfig.Transport.FRAMED;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class DriftNettyClientConfig
{
    public enum Transport
    {
        UNFRAMED, FRAMED, HEADER
    }

    public enum Protocol
    {
        BINARY, COMPACT
    }

    private Transport transport = FRAMED;
    private Protocol protocol = BINARY;
    private int maxFrameSize = toIntExact(new DataSize(16, MEGABYTE).toBytes());

    private Duration connectTimeout = new Duration(500, MILLISECONDS);
    private Duration requestTimeout = new Duration(1, MINUTES);

    private HostAndPort socksProxy;

    private boolean poolEnabled;

    private boolean sslEnabled;
    private List<String> ciphers;

    private File trustCertificate;
    private File key;
    private String keyPassword;
    private long sessionCacheSize = 10_000;
    private long sessionTimeoutSeconds = 86_400;

    @NotNull
    public Transport getTransport()
    {
        return transport;
    }

    @Config("thrift.client.transport")
    public DriftNettyClientConfig setTransport(Transport transport)
    {
        this.transport = transport;
        return this;
    }

    @NotNull
    public Protocol getProtocol()
    {
        return protocol;
    }

    @Config("thrift.client.protocol")
    public DriftNettyClientConfig setProtocol(Protocol protocol)
    {
        this.protocol = protocol;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("thrift.client.connect-timeout")
    public DriftNettyClientConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("thrift.client.request-timeout")
    public DriftNettyClientConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public HostAndPort getSocksProxy()
    {
        return socksProxy;
    }

    @Config("thrift.client.socks-proxy")
    public DriftNettyClientConfig setSocksProxy(HostAndPort socksProxy)
    {
        this.socksProxy = socksProxy;
        return this;
    }

    @Min(0)
    @Max(0x3FFFFFFF)
    public int getMaxFrameSize()
    {
        return maxFrameSize;
    }

    @Config("thrift.client.max-frame-size")
    public DriftNettyClientConfig setMaxFrameSize(int maxFrameSize)
    {
        checkArgument(maxFrameSize <= 0x3FFFFFFF);
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    public boolean isPoolEnabled()
    {
        return poolEnabled;
    }

    @Config("thrift.client.pool-enabled")
    public DriftNettyClientConfig setPoolEnabled(boolean poolEnabled)
    {
        this.poolEnabled = poolEnabled;
        return this;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    @Config("thrift.client.ssl.enabled")
    public DriftNettyClientConfig setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public File getTrustCertificate()
    {
        return trustCertificate;
    }

    @Config("thrift.client.ssl.trust-certificate")
    public DriftNettyClientConfig setTrustCertificate(File trustCertificate)
    {
        this.trustCertificate = trustCertificate;
        return this;
    }

    public File getKey()
    {
        return key;
    }

    @Config("thrift.client.ssl.key")
    public DriftNettyClientConfig setKey(File key)
    {
        this.key = key;
        return this;
    }

    public String getKeyPassword()
    {
        return keyPassword;
    }

    @Config("thrift.client.ssl.key-password")
    public DriftNettyClientConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public long getSessionCacheSize()
    {
        return sessionCacheSize;
    }

    @Config("thrift.client.ssl.session-cache-size")
    public DriftNettyClientConfig setSessionCacheSize(long sessionCacheSize)
    {
        this.sessionCacheSize = sessionCacheSize;
        return this;
    }

    public long getSessionTimeoutSeconds()
    {
        return sessionTimeoutSeconds;
    }

    @Config("thrift.client.ssl.session-timeout-seconds")
    public DriftNettyClientConfig setSessionTimeoutSeconds(long sessionTimeoutSeconds)
    {
        this.sessionTimeoutSeconds = sessionTimeoutSeconds;
        return this;
    }

    public List<String> getCiphers()
    {
        return ciphers;
    }

    @Config("thrift.client.ssl.ciphers")
    public DriftNettyClientConfig setCiphers(String ciphers)
    {
        this.ciphers = Splitter
                .on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(requireNonNull(ciphers, "ciphers is null"));
        return this;
    }
}
