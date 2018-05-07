package org.infinispan.rest.http2;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.KeyManagerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.AsciiString;

/**
 * HTTP/2 client based on Netty.
 *
 * <p>
 *    Unfortunately it is very hard to get a good HTTP/2 client with ALPN support. All major implementations require
 *    using Jetty Java Agent (which needs to be applied into bootclasspath. This is very inconvenient. Thankfully Netty
 *    can use OpenSSL but the downside is that it contains lots and lots of boilerplate code.
 *
 *    It might be a good idea to replace this implementation once JDK9 is mainstream and use something better.
 * </p>
 *
 * @author Sebastian Łaskawiec
 * @link https://github.com/fstab/http2-examples/tree/master/multiplexing-examples/netty-client/src/main/java/de/consol/labs/h2c/examples/client/netty
 */
public class Http2Client {

   final SslContext sslCtx;
   final EventLoopGroup workerGroup = new NioEventLoopGroup();

   Channel channel;
   Http2ClientInitializer initializer;
   //Netty uses stream ids to separate concurrent conversations. It seems to be an implementation details but this counter
   //get always incremented by 2
   AtomicInteger streamCounter = new AtomicInteger(3);
   AsciiString hostname;


   public Http2Client(SslContext sslCtx) {
      this.sslCtx = sslCtx;
   }

   public static Http2Client newClientWithHttp11Upgrade() {
      return new Http2Client(null);
   }

   public static Http2Client newClientWithAlpn(String keystorePath, String keystorePassword) throws Exception {
      KeyStore ks = KeyStore.getInstance("pkcs12");
      ks.load(new FileInputStream(keystorePath), keystorePassword.toCharArray());
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, keystorePassword.toCharArray());

      SslProvider provider = OpenSsl.isAlpnSupported() ? SslProvider.OPENSSL : SslProvider.JDK;
      SslContext sslCtx = SslContextBuilder.forClient()
            .sslProvider(provider)
            .keyManager(kmf)
            .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocolConfig(new ApplicationProtocolConfig(
                  ApplicationProtocolConfig.Protocol.ALPN,
                  ApplicationProtocolConfig.SelectorFailureBehavior.CHOOSE_MY_LAST_PROTOCOL,
                  ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                  ApplicationProtocolNames.HTTP_2))
            .build();

      return new Http2Client(sslCtx);
   }

   public void start(String host, int port) throws Exception {
      hostname = new AsciiString(host + ':' + port);
      initializer = new Http2ClientInitializer(sslCtx, Integer.MAX_VALUE);

      Bootstrap b = new Bootstrap();
      b.group(workerGroup);
      b.channel(NioSocketChannel.class);
      b.option(ChannelOption.SO_KEEPALIVE, true);

      b.remoteAddress(host, port);
      b.handler(initializer);

      // Start the client.
      channel = b.connect().syncUninterruptibly().channel();
      System.out.println("Connected to [" + host + ':' + port + ']');

      // Wait for the HTTP/2 upgrade to occur.
      Http2SettingsHandler http2SettingsHandler = initializer.settingsHandler();
      http2SettingsHandler.awaitSettings(15, TimeUnit.SECONDS);
   }

   public void stop() {
      workerGroup.shutdownGracefully();
   }

   public void awaitForResponses() {
      HttpResponseHandler responseHandler = initializer.responseHandler();
      responseHandler.awaitResponses(60, TimeUnit.SECONDS);
   }

   public Queue<FullHttpResponse> getResponses() {
      awaitForResponses();
      HttpResponseHandler responseHandler = initializer.responseHandler();
      return responseHandler.getResponses();
   }

   public void sendRequest(FullHttpRequest request) {
      HttpResponseHandler responseHandler = initializer.responseHandler();
      int streamId = streamCounter.getAndAdd(2);
      HttpScheme scheme = sslCtx != null ? HttpScheme.HTTPS : HttpScheme.HTTP;

      request.headers().add(HttpHeaderNames.HOST, hostname);
      request.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), scheme.name());
      request.headers().add(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
      responseHandler.put(streamId, channel.write(request), channel.newPromise());
      channel.flush();
   }

}
