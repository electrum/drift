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
package io.airlift.drift.transport.apache;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.drift.transport.AddressSelector;
import io.airlift.drift.transport.MethodInvoker;
import io.airlift.drift.transport.MethodInvokerFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.Closeable;
import java.security.KeyStore;
import java.security.Security;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.airlift.drift.transport.apache.PemReader.loadKeyStore;
import static io.airlift.drift.transport.apache.PemReader.loadTrustStore;
import static java.util.Objects.requireNonNull;

public class ApacheThriftMethodInvokerFactory<I>
        implements MethodInvokerFactory<I>, Closeable
{
    private final Function<I, ApacheThriftClientConfig> clientConfigurationProvider;

    private final ListeningExecutorService executorService;

    public static ApacheThriftMethodInvokerFactory<?> createStaticApacheThriftMethodInvokerFactory(ApacheThriftClientConfig clientConfig)
    {
        return createStaticApacheThriftMethodInvokerFactory(clientConfig, new ApacheThriftConnectionFactoryConfig());
    }

    public static ApacheThriftMethodInvokerFactory<?> createStaticApacheThriftMethodInvokerFactory(
            ApacheThriftClientConfig clientConfig,
            ApacheThriftConnectionFactoryConfig factoryConfig)
    {
        requireNonNull(clientConfig, "clientConfig is null");
        return new ApacheThriftMethodInvokerFactory<>(factoryConfig, clientIdentity -> clientConfig);
    }

    @Inject
    public ApacheThriftMethodInvokerFactory(ApacheThriftConnectionFactoryConfig factoryConfig, Function<I, ApacheThriftClientConfig> clientConfigurationProvider)
    {
        requireNonNull(factoryConfig, "factoryConfig is null");

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("drift-client-%s")
                .setDaemon(true)
                .build();

        if (factoryConfig.getThreadCount() == null) {
            executorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(threadFactory));
        }
        else {
            executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(factoryConfig.getThreadCount(), threadFactory));
        }
        this.clientConfigurationProvider = requireNonNull(clientConfigurationProvider, "clientConfigurationProvider is null");
    }

    @Override
    public MethodInvoker createMethodInvoker(AddressSelector addressSelector, I clientIdentity)
    {
        ApacheThriftClientConfig config = clientConfigurationProvider.apply(clientIdentity);

        TTransportFactory transportFactory;
        switch (config.getTransport()) {
            case UNFRAMED:
                transportFactory = new TTransportFactory();
                break;
            case FRAMED:
                transportFactory = new TFramedTransport.Factory();
                break;
            default:
                throw new IllegalArgumentException("Unknown transport: " + config.getTransport());
        }

        TProtocolFactory protocolFactory;
        switch (config.getProtocol()) {
            case BINARY:
                protocolFactory = new TBinaryProtocol.Factory();
                break;
            case COMPACT:
                protocolFactory = new TCompactProtocol.Factory();
                break;
            default:
                throw new IllegalArgumentException("Unknown protocol: " + config.getProtocol());
        }

        Optional<SSLContext> sslContext = Optional.empty();
        if (config.isSslEnabled()) {
            sslContext = Optional.of(createSslContext(config));
        }

        return new ApacheThriftMethodInvoker(
                executorService,
                transportFactory,
                protocolFactory,
                addressSelector,
                config.getConnectTimeout(),
                config.getRequestTimeout(),
                Optional.ofNullable(config.getSocksProxy()),
                sslContext);
    }

    private static SSLContext createSslContext(ApacheThriftClientConfig config)
    {
        try {
            KeyStore trustStore = loadTrustStore(config.getTrustCertificate());
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            KeyManager[] keyManagers = null;
            if (config.getKey() != null) {
                Optional<String> keyPassword = Optional.ofNullable(config.getKeyPassword());
                KeyStore keyStore = loadKeyStore(config.getTrustCertificate(), config.getKey(), keyPassword);
                String algorithm = MoreObjects.firstNonNull(Security.getProperty("ssl.KeyManagerFactory.algorithm"), "SunX509");
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
                keyManagerFactory.init(keyStore, new char[0]);
                keyManagers = keyManagerFactory.getKeyManagers();
            }

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);
            return sslContext;
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unable to load SSL keys", e);
        }
    }

    @PreDestroy
    @Override
    public void close()
    {
        MoreExecutors.shutdownAndAwaitTermination(executorService, 5, TimeUnit.MINUTES);
    }
}
