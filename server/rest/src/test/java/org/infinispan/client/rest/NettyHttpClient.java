package org.infinispan.client.rest;

import java.io.Closeable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.ServerConfiguration;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.ssl.SslContext;

/**
 * HTTP/2 client based on Netty.
 *
 * <p>
 * Unfortunately it is very hard to get a good HTTP/2 client with ALPN support. All major implementations require using
 * Jetty Java Agent (which needs to be applied into bootclasspath. This is very inconvenient. Thankfully Netty can use
 * OpenSSL but the downside is that it contains lots and lots of bolerplate code.
 * <p>
 * It might be a good idea to replace this implementation once JDK9 is mainstream and use something better.
 * </p>
 *
 * @author Sebastian Łaskawiec
 * @link https://github.com/fstab/http2-examples/tree/master/multiplexing-examples/netty-client/src/main/java/de/consol/labs/h2c/examples/client/netty
 */
public class NettyHttpClient implements Closeable {
   private final CommunicationInitializer initializer;
   private final SslContext sslCtx;
   private final EventLoopGroup workerGroup = new NioEventLoopGroup();
   private final RestClientConfiguration configuration;
   private Channel channel;

   public NettyHttpClient(RestClientConfiguration configuration) {
      this.configuration = configuration;
      this.sslCtx = NettyTruststoreUtil.createSslContext(configuration);
      switch (configuration.protocol()) {
         case HTTP_11:
            initializer = new Http11ClientInitializer(sslCtx, Integer.MAX_VALUE);
            break;
         case HTTP_20:
            initializer = new Http2ClientInitializer(sslCtx, Integer.MAX_VALUE, this.configuration.security().ssl().sniHostName());
            break;
         default:
            throw new IllegalArgumentException(); // unreachable, but please the final field gods
      }
      start();
   }

   private void start() {
      Bootstrap b = new Bootstrap();
      b.group(workerGroup);
      b.channel(NioSocketChannel.class);
      b.option(ChannelOption.SO_KEEPALIVE, configuration.tcpKeepAlive());
      b.option(ChannelOption.SO_TIMEOUT, configuration.socketTimeout());
      b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectionTimeout());
      b.option(ChannelOption.TCP_NODELAY, configuration.tcpNoDelay());
      ServerConfiguration serverConfiguration = configuration.servers().get(0);
      b.remoteAddress(serverConfiguration.host(), serverConfiguration.port());
      b.handler(initializer);

      // Start the client.
      channel = b.connect().syncUninterruptibly().channel();

      initializer.upgradeToHttp2IfNeeded();
   }

   public void stop() {
      try {
         // Make the quiet period 100ms instead of 2s (Netty default)
         workerGroup.shutdownGracefully(100, 15000, TimeUnit.MILLISECONDS).sync();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IllegalLifecycleStateException(e);
      }
   }

   public CompletionStage<FullHttpResponse> sendRequest(FullHttpRequest request) {
      return initializer.getCommunicationHandler().sendRequest(request, sslCtx, channel);
   }

   @Override
   public void close() {
      stop();
   }

   public RestClientConfiguration getConfiguration() {
      return configuration;
   }
}
