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

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.apache.thrift.transport.TTransportException;

import java.net.InetSocketAddress;
import java.util.Optional;

import static com.google.common.primitives.Ints.saturatedCast;
import static io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;
import static java.util.Objects.requireNonNull;
import static org.apache.thrift.transport.TTransportException.NOT_OPEN;

class ConnectionFactory
        implements ConnectionManager
{
    private final EventLoopGroup group;
    private final MessageFraming messageFraming;
    private final MessageEncoding messageEncoding;
    private final Optional<SslContext> sslContext;

    private final Duration connectTimeout;
    private final Duration requestTimeout;
    private final Optional<HostAndPort> socksProxy;

    ConnectionFactory(
            EventLoopGroup group,
            MessageFraming messageFraming,
            MessageEncoding messageEncoding,
            Optional<SslContext> sslContext,
            DriftNettyClientConfig clientConfig)
    {
        this.group = requireNonNull(group, "group is null");
        this.messageFraming = requireNonNull(messageFraming, "messageFraming is null");
        this.messageEncoding = requireNonNull(messageEncoding, "messageEncoding is null");
        this.sslContext = requireNonNull(sslContext, "sslContext is null");

        requireNonNull(clientConfig, "clientConfig is null");
        this.connectTimeout = clientConfig.getConnectTimeout();
        this.requestTimeout = clientConfig.getRequestTimeout();
        this.socksProxy = Optional.ofNullable(clientConfig.getSocksProxy());
    }

    @Override
    public Future<Channel> getConnection(HostAndPort address)
    {
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(CONNECT_TIMEOUT_MILLIS, saturatedCast(connectTimeout.toMillis()))
                    .handler(new ThriftClientInitializer(
                            messageFraming,
                            messageEncoding,
                            requestTimeout,
                            socksProxy,
                            sslContext));

            Promise<Channel> promise = group.next().newPromise();
            bootstrap.connect(new InetSocketAddress(address.getHost(), address.getPort()))
                    .addListener((ChannelFutureListener) future -> notifyConnect(future, promise));
            return promise;
        }
        catch (Throwable e) {
            return group.next().newFailedFuture(new TTransportException(NOT_OPEN, e));
        }
    }

    private static void notifyConnect(ChannelFuture future, Promise<Channel> promise)
    {
        if (future.isSuccess()) {
            Channel channel = future.channel();
            if (!promise.trySuccess(channel)) {
                // Promise was completed in the meantime (like cancelled), just release the channel again
                channel.close();
            }
        }
        else {
            promise.tryFailure(future.cause());
        }
    }

    @Override
    public void returnConnection(Channel connection)
    {
        connection.close();
    }
}
