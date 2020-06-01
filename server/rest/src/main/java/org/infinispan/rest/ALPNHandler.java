package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.IF_MODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpHeaderNames.IF_NONE_MATCH;
import static io.netty.handler.codec.http.HttpHeaderNames.IF_UNMODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static org.infinispan.rest.NettyRestRequest.CREATED_HEADER;
import static org.infinispan.rest.NettyRestRequest.EXTENDED_HEADER;
import static org.infinispan.rest.NettyRestRequest.FLAGS_HEADER;
import static org.infinispan.rest.NettyRestRequest.KEY_CONTENT_TYPE_HEADER;
import static org.infinispan.rest.NettyRestRequest.LAST_USED_HEADER;
import static org.infinispan.rest.NettyRestRequest.MAX_TIME_IDLE_HEADER;
import static org.infinispan.rest.NettyRestRequest.TTL_SECONDS_HEADER;
import static org.infinispan.rest.RestChannelInitializer.MAX_HEADER_SIZE;
import static org.infinispan.rest.RestChannelInitializer.MAX_INITIAL_LINE_SIZE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler.UpgradeCodecFactory;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.codec.http2.CleartextHttp2ServerUpgradeHandler;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2MultiplexCodec;
import io.netty.handler.codec.http2.Http2MultiplexCodecBuilder;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.AsciiString;

/**
 * Handler responsible for TLS/ALPN negotiation.
 *
 * @author Sebastian Łaskawiec
 */
@ChannelHandler.Sharable
public class ALPNHandler extends ApplicationProtocolNegotiationHandler {

   private static final int CROSS_ORIGIN_ALT_PORT = 9000;
   private static final String[] SCHEMES = new String[]{"http", "https"};

   protected final RestServer restServer;

   public ALPNHandler(RestServer restServer) {
      super(ApplicationProtocolNames.HTTP_1_1);
      this.restServer = restServer;
   }

   @Override
   public void configurePipeline(ChannelHandlerContext ctx, String protocol) {
      configurePipeline(ctx.pipeline(), protocol);
   }

   public void configurePipeline(ChannelPipeline pipeline, String protocol) {
      if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
         configureHttp2(pipeline);
         return;
      }

      if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
         configureHttp1(pipeline);
         return;
      }

      throw new IllegalStateException("unknown protocol: " + protocol);
   }

   /**
    * Configure pipeline for HTTP/2 after negotiated via ALPN
    */
   protected void configureHttp2(ChannelPipeline pipeline) {
      pipeline.addLast(getHttp11To2ConnectionHandler());
      pipeline.addLast("rest-handler-http2", new RestRequestHandler(restServer));
   }

   /**
    * Configure pipeline for HTTP/1.1 after negotiated by ALPN
    */
   protected void configureHttp1(ChannelPipeline pipeline) {
      Http2MultiplexCodec multiplexCodec = Http2MultiplexCodecBuilder.forServer(new ChannelInitializer<Channel>() {
         @Override
         protected void initChannel(Channel channel) {
            ChannelPipeline p = channel.pipeline();
            p.addLast(new Http2StreamFrameToHttpObjectCodec(true));
            p.addLast(new HttpObjectAggregator(maxContentLength()));
            p.addLast(new ChunkedWriteHandler());
            p.addLast(new RestRequestHandler(restServer));
         }
      }).initialSettings(Http2Settings.defaultSettings()).build();

      UpgradeCodecFactory upgradeCodecFactory = protocol -> {
         if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
            return new Http2ServerUpgradeCodec(multiplexCodec);
         } else {
            return null;
         }
      };

      HttpServerCodec httpCodec = new HttpServerCodec(MAX_INITIAL_LINE_SIZE, MAX_HEADER_SIZE, maxContentLength());
      HttpServerUpgradeHandler upgradeHandler = new HttpServerUpgradeHandler(httpCodec, upgradeCodecFactory, maxContentLength());
      CleartextHttp2ServerUpgradeHandler cleartextHttp2ServerUpgradeHandler = new CleartextHttp2ServerUpgradeHandler(httpCodec, upgradeHandler, multiplexCodec);
      pipeline.addLast(cleartextHttp2ServerUpgradeHandler);

      pipeline.addLast(new HttpContentCompressor(restServer.getConfiguration().getCompressionLevel()));
      pipeline.addLast(new HttpObjectAggregator(maxContentLength()));
      List<CorsConfig> configuredRules = restServer.getConfiguration().getCorsRules();
      List<CorsConfig> rules = addLocalhostPermissions(configuredRules, restServer.getPort(), CROSS_ORIGIN_ALT_PORT);
      pipeline.addLast(new CorsHandler(rules, true));
      pipeline.addLast(new ChunkedWriteHandler());
      pipeline.addLast(new Http11RequestHandler(restServer));
   }

   private List<CorsConfig> addLocalhostPermissions(List<CorsConfig> corsRules, int... ports) {
      List<CorsConfig> configs = new ArrayList<>(corsRules);
      for (int port : ports) {
         for (String scheme : SCHEMES) {
            String localIpv4 = scheme + "://" + "127.0.0.1" + ":" + port;
            String localDomain = scheme + "://" + "localhost" + ":" + port;
            String localIpv6 = scheme + "://" + "[::1]" + ":" + port;
            CorsConfig config = CorsConfigBuilder.forOrigins(localIpv4, localDomain, localIpv6)
                  .allowCredentials()
                  .allowedRequestMethods(GET, POST, PUT, DELETE, HEAD, OPTIONS)
                  // Not all browsers support "*" (https://github.com/whatwg/fetch/issues/251) so we need to add each
                  // header individually
                  .allowedRequestHeaders(CACHE_CONTROL, CONTENT_TYPE, CREATED_HEADER, EXTENDED_HEADER, FLAGS_HEADER,
                        IF_MODIFIED_SINCE, IF_UNMODIFIED_SINCE, IF_NONE_MATCH, KEY_CONTENT_TYPE_HEADER,
                        MAX_TIME_IDLE_HEADER, LAST_USED_HEADER, TTL_SECONDS_HEADER)
                  .exposeHeaders(ResponseHeader.toArray())
                  .build();
            configs.add(config);
         }
      }
      return Collections.unmodifiableList(configs);
   }

   protected int maxContentLength() {
      return this.restServer.getConfiguration().maxContentLength() + MAX_INITIAL_LINE_SIZE + MAX_HEADER_SIZE;
   }

   /**
    * Creates a handler to translates between HTTP/1.x objects and HTTP/2 frames
    *
    * @return new instance of {@link HttpToHttp2ConnectionHandler}.
    */
   private HttpToHttp2ConnectionHandler getHttp11To2ConnectionHandler() {
      DefaultHttp2Connection connection = new DefaultHttp2Connection(true);

      InboundHttp2ToHttpAdapter listener = new InboundHttp2ToHttpAdapterBuilder(connection)
            .propagateSettings(true)
            .validateHttpHeaders(false)
            .maxContentLength(maxContentLength())
            .build();

      return new HttpToHttp2ConnectionHandlerBuilder()
            .frameListener(listener)
            .connection(connection)
            .build();
   }

   public ChannelHandler getHttp1Handler() {
      return new Http11RequestHandler(restServer);
   }

   public ApplicationProtocolConfig getAlpnConfiguration() {
      if (restServer.getConfiguration().ssl().enabled()) {
         return new ApplicationProtocolConfig(
               ApplicationProtocolConfig.Protocol.ALPN,
               // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
               ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
               // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
               ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
               ApplicationProtocolNames.HTTP_2,
               ApplicationProtocolNames.HTTP_1_1);
      }
      return null;
   }
}
