package org.infinispan.rest.http2;

import java.util.Queue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;

/**
 * HTTP/2 client based on Netty.
 *
 * <p>
 *    Unfortunately it is very hard to get a good HTTP/2 client with ALPN support. All major implementations require
 *    using Jetty Java Agent (which needs to be applied into bootclasspath. This is very inconvenient. Thankfully Netty
 *    can use OpenSSL but the downside is that it contains lots and lots of bolerplate code.
 *
 *    It might be a good idea to replace this implementation once JDK9 is mainstream and use something better.
 * </p>
 *
 * @author Sebastian Łaskawiec
 * @see https://github.com/fstab/http2-examples/tree/master/multiplexing-examples/netty-client/src/main/java/de/consol/labs/h2c/examples/client/netty
 */
public class NettyHttpClient {

   private final SslContext sslCtx;
   private final EventLoopGroup workerGroup = new NioEventLoopGroup();

   private Channel channel;
   private CommunicationInitializer initializer;


   public NettyHttpClient(SslContext sslCtx, CommunicationInitializer initializer) {
      this.sslCtx = sslCtx;
      this.initializer = initializer;
   }

   public static NettyHttpClient newHttp2ClientWithHttp11Upgrade() {
      return new NettyHttpClient(null, new Http2ClientInitializer(null, Integer.MAX_VALUE, null));
   }

   public static NettyHttpClient newHttp11Client() {
      return new NettyHttpClient(null, new Http11ClientInitializer(null, Integer.MAX_VALUE));
   }

   public static NettyHttpClient newHttp2ClientWithALPN(String keystorePath, String keystorePassword) throws Exception {
      return newHttp2ClientWithALPN(keystorePath, keystorePassword, null);
   }

   public static NettyHttpClient newHttp2ClientWithALPN(String keystorePath, String keystorePassword, String sniName) throws Exception {
      SslContext sslContext = NettyTruststoreUtil.createTruststoreContext(keystorePath, keystorePassword.toCharArray(), ApplicationProtocolNames.HTTP_2);
      return new NettyHttpClient(sslContext, new Http2ClientInitializer(sslContext, Integer.MAX_VALUE, sniName));
   }

   public void start(String host, int port) throws Exception {
      Bootstrap b = new Bootstrap();
      b.group(workerGroup);
      b.channel(NioSocketChannel.class);
      b.option(ChannelOption.SO_KEEPALIVE, true);

      b.remoteAddress(host, port);
      b.handler(initializer);

      // Start the client.
      channel = b.connect().syncUninterruptibly().channel();
      System.out.println("Connected to [" + host + ':' + port + ']');

      initializer.upgradeToHttp2IfNeeded();
   }

   public void stop() {
      workerGroup.shutdownGracefully();
   }

   public Queue<FullHttpResponse> getResponses() {
      return initializer.getCommunicationHandler().getResponses();
   }

   public void sendRequest(FullHttpRequest request) {
      initializer.getCommunicationHandler().sendRequest(request, sslCtx, channel);
   }

}
