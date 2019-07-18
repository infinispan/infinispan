package org.infinispan.rest.client;

import static io.netty.handler.logging.LogLevel.INFO;
import static io.netty.handler.ssl.ApplicationProtocolNames.HTTP_1_1;
import static io.netty.handler.ssl.ApplicationProtocolNames.HTTP_2;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.ServerConfiguration;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;

/**
 * An HTTP client that supports both Http1 and Http2, including clear text upgrades
 *
 * @since 10.0
 */
public final class NettyHttpClient {
   private final HttpClientInitializer initializer;
   private final boolean enableSSL;
   private final String protocol;
   private final String sniName;
   private final boolean http2;
   private Channel channel;
   private AtomicInteger streamId = new AtomicInteger(1);
   private final EventLoopGroup workerGroup;
   private final Bootstrap b;
   private SslContext sslCtx;
   private volatile boolean handshake, priorKnowledge;
   private static final int HANDSHAKE_DELAY_SECONDS = 5;

   public static NettyHttpClient forConfiguration(RestClientConfiguration configuration) {
      ServerConfiguration serverConfiguration = configuration.servers().iterator().next();
      String host = serverConfiguration.host();
      int port = serverConfiguration.port();
      boolean priorKnowledge = configuration.priorKnowledge();
      String protocol = configuration.protocol().equals(HTTP_11) ? HTTP_1_1 : HTTP_2;
      boolean security = configuration.security().ssl().enabled();
      try {
         SslContext sslContext = NettyTruststoreUtil.createSslContext(configuration);
         String sniName = configuration.security().ssl().sniHostName();
         return new NettyHttpClient(host, port, security, priorKnowledge, protocol, sslContext, sniName);
      } catch (Exception e) {
         throw new RuntimeException("Error creating client", e);
      }
   }

   private NettyHttpClient(String host, int port, boolean enableSSL, boolean priorKnowledge, String protocol, SslContext sslContext, String sniName) {
      this.sslCtx = sslContext;
      this.priorKnowledge = priorKnowledge;
      this.enableSSL = enableSSL;
      this.protocol = protocol;
      this.sniName = sniName;
      this.workerGroup = new NioEventLoopGroup();
      this.initializer = new HttpClientInitializer();
      this.http2 = HTTP_2.equals(protocol);

      b = new Bootstrap();
      b.group(workerGroup);
      b.channel(NioSocketChannel.class);
      b.option(ChannelOption.SO_KEEPALIVE, true);
      b.remoteAddress(host, port);
      b.handler(initializer);
      channel = b.connect().syncUninterruptibly().channel();
   }

   public CompletionStage<FullHttpResponse> sendRequest(FullHttpRequest request) {
      if (!handshake && http2) {
         synchronized (this) {
            if (!handshake) {
               if (enableSSL) {
                  initializer.settingsHandler.awaitSettings();
               } else {
                  int streamId = this.streamId.getAndAdd(2);
                  CompletionStage<FullHttpResponse> response = sendRequestInternal(request, streamId);
                  FullHttpResponse fullHttpResponse;
                  try {
                     fullHttpResponse = response.toCompletableFuture().get(HANDSHAKE_DELAY_SECONDS, TimeUnit.SECONDS);
                  } catch (InterruptedException | ExecutionException | TimeoutException e) {
                     throw new RuntimeException("Timeout waiting for the handshake", e);
                  }
                  return CompletableFuture.completedFuture(fullHttpResponse);
               }
               handshake = true;
            }
         }
      }
      return sendRequestInternal(request, streamId.getAndAdd(2));
   }

   private CompletionStage<FullHttpResponse> sendRequestInternal(FullHttpRequest request, int streamId) {
      boolean open = channel.isOpen();
      if (!open) channel = b.connect().syncUninterruptibly().channel();

      if (http2) request.headers().set(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);

      request.headers().set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), enableSSL ? "https" : "http");
      request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
      request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.TEXT_PLAIN);
      HttpUtil.setContentLength(request, request.content().readableBytes());
      CompletableFuture<FullHttpResponse> promise = new CompletableFuture<>();
      initializer.responseHandler().registerRequest(streamId, promise);
      ChannelFuture channelFuture = channel.writeAndFlush(request);
      channelFuture.addListener((ChannelFutureListener) future -> {
         if (!future.isSuccess()) {
            future.cause().printStackTrace();
         }
      });
      return promise;
   }

   /**
    * Shutdown the client and close underlying resources
    */
   public void stop() {
      channel.close();
      channel.closeFuture().syncUninterruptibly();
      workerGroup.shutdownGracefully().syncUninterruptibly();
   }


   abstract static class ResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

      /**
       * Registers a promise that will be completed when the response arrives
       */
      abstract void registerRequest(int requestId, CompletableFuture<FullHttpResponse> promise);
   }

   /**
    * Response callback for HTTP/2.0 requests
    */
   static class Http20ResponseHandler extends ResponseHandler {
      private final Map<Integer, CompletableFuture<FullHttpResponse>> responseMap = new ConcurrentHashMap<>();

      void registerRequest(int streamId, CompletableFuture<FullHttpResponse> promise) {
         responseMap.put(streamId, promise);
      }

      @Override
      protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
         Integer streamId = msg.headers().getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
         if (streamId == null) {
            System.err.println("HttpResponseHandler unexpected message received: " + msg);
            return;
         }
         CompletableFuture<FullHttpResponse> future = responseMap.remove(streamId);
         if (future == null) {
            System.err.println("Message received for unknown stream id " + streamId);
         } else {
            future.complete(msg);
         }
      }
   }

   /**
    * Response callback for HTTP/1.1 requests
    */
   static class Http11ResponseHandler extends ResponseHandler {
      private AtomicReference<CompletableFuture<FullHttpResponse>> futureRef = new AtomicReference<>();

      void registerRequest(int streamId, CompletableFuture<FullHttpResponse> promise) {
         futureRef.set(promise);
      }

      @Override
      protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
         futureRef.get().complete(msg);
      }
   }

   /**
    * Build the pipeline based on the client requirements: TLS, HTTP/1.1, HTTP/2.0 with clear text and ALPN upgrade
    */
   class HttpClientInitializer extends ChannelInitializer<SocketChannel> {
      private HttpToHttp2ConnectionHandler connectionHandler;
      private ResponseHandler responseHandler;
      SettingsHandler settingsHandler;


      @Override
      public void initChannel(SocketChannel ch) {
         settingsHandler = new SettingsHandler(ch.newPromise());
         if (protocol.equals(HTTP_1_1)) {
            configureHttp1(ch);
         } else if (protocol.equals(HTTP_2)) {
            configureHttp2(ch);
         } else {
            throw new IllegalArgumentException("Unsupported protocol" + protocol);
         }
      }

      private void configureHttp1(SocketChannel ch) {
         responseHandler = new Http11ResponseHandler();
         ChannelPipeline p = ch.pipeline();
         if (sslCtx != null) {
            p.addLast(sslCtx.newHandler(ch.alloc()));
         }
         p.addLast(new HttpClientCodec());
         p.addLast(new HttpContentDecompressor());
         p.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
         p.addLast(responseHandler);
      }

      private void configureHttp2(SocketChannel ch) {
         responseHandler = new Http20ResponseHandler();
         final Http2Connection connection = new DefaultHttp2Connection(false);
         connectionHandler = new HttpToHttp2ConnectionHandlerBuilder().frameListener(
               new DelegatingDecompressorFrameListener(connection,
                     new InboundHttp2ToHttpAdapterBuilder(connection)
                           .maxContentLength(Integer.MAX_VALUE)
                           .propagateSettings(true)
                           .build()))
               .frameLogger(new Http2FrameLogger(INFO, HttpClientInitializer.class))
               .connection(connection)
               .build();
         if (sslCtx != null) {
            configureSecureHttp2(ch);
         } else {
            if (!priorKnowledge) {
               configureSimpleHttp2(ch);
            } else {
               configurePriorKnowledgeHttp2(ch);
            }
         }
      }

      private void configureSecureHttp2(SocketChannel ch) {
         ChannelPipeline pipeline = ch.pipeline();
         pipeline.addLast(sslCtx.newHandler(ch.alloc(), sniName, -1));
         pipeline.addLast(new ApplicationProtocolNegotiationHandler("") {
            @Override
            protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
               if (HTTP_2.equals(protocol)) {
                  ChannelPipeline p = ctx.pipeline();
                  p.addLast(connectionHandler);
                  p.addLast(settingsHandler);
                  p.addLast(responseHandler);
                  return;
               }
               ctx.close();
               throw new IllegalStateException("unknown protocol: " + protocol);
            }
         });
      }

      private void configureSimpleHttp2(SocketChannel ch) {
         HttpClientCodec sourceCodec = new HttpClientCodec();
         Http2ClientUpgradeCodec upgradeCodec = new Http2ClientUpgradeCodec(connectionHandler);
         HttpClientUpgradeHandler upgradeHandler = new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, 65536);
         ch.pipeline().addLast(sourceCodec, upgradeHandler, new UserEventLogger());
         ch.pipeline().addLast(responseHandler);
      }

      private void configurePriorKnowledgeHttp2(SocketChannel ch) {
         ch.pipeline().addLast(connectionHandler, new UserEventLogger());
         ch.pipeline().addLast(responseHandler);
      }

      ResponseHandler responseHandler() {
         return responseHandler;
      }
   }

   private static class UserEventLogger extends ChannelInboundHandlerAdapter {
      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
         System.out.println("User Event Triggered: " + evt);
         ctx.fireUserEventTriggered(evt);
      }
   }

   /**
    * A temporary handler to wait on the HTTP2-Settings exchange
    */
   static class SettingsHandler extends SimpleChannelInboundHandler<Http2Settings> {
      private final ChannelPromise promise;

      SettingsHandler(ChannelPromise promise) {
         this.promise = promise;
      }

      void awaitSettings() {
         if (!promise.awaitUninterruptibly(HANDSHAKE_DELAY_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for settings");
         }
         if (!promise.isSuccess()) {
            throw new RuntimeException(promise.cause());
         }
      }

      @Override
      protected void channelRead0(ChannelHandlerContext ctx, Http2Settings msg) {
         promise.setSuccess();
         ctx.pipeline().remove(this);
      }
   }
}
