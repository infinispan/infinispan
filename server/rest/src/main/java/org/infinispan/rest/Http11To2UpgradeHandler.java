package org.infinispan.rest;

import java.util.List;

import org.infinispan.rest.configuration.RestServerConfiguration;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.util.AsciiString;

/**
 * Handler responsible for TLS/ALPN negotiation as well as HTTP/1.1 Upgrade header handling
 *
 * @author Sebastian Łaskawiec
 */
@ChannelHandler.Sharable
public class Http11To2UpgradeHandler extends ApplicationProtocolNegotiationHandler {
   private static final int MAX_INITIAL_LINE_SIZE = 4096;
   private static final int MAX_HEADER_SIZE = 8192;

   private final RestServer restServer;
   private final List<CorsConfig> corsRules;

   Http11To2UpgradeHandler(RestServer restServer) {
      super(ApplicationProtocolNames.HTTP_1_1);
      this.restServer = restServer;
      this.corsRules = restServer.getConfiguration().getCorsRules();
   }

   @Override
   protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
      configurePipeline(ctx.pipeline(), protocol);
   }

   void configurePipeline(ChannelPipeline pipeline, String protocol) {
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

   private void configureHttp2(ChannelPipeline pipeline) {
      pipeline.addLast(getHttp11To2ConnectionHandler());
      pipeline.addLast("rest-handler-http2", getHttp2Handler());
   }

   private void configureHttp1(ChannelPipeline pipeline) {
      RestServerConfiguration configuration = restServer.getConfiguration();
      final HttpServerCodec httpCodec = new HttpServerCodec(MAX_INITIAL_LINE_SIZE, MAX_HEADER_SIZE, configuration.maxContentLength());
      pipeline.addLast(httpCodec);
      if(!corsRules.isEmpty()) {
         pipeline.addLast(new org.infinispan.rest.cors.CorsHandler(corsRules, true));
      }
      pipeline.addLast(new HttpContentCompressor(configuration.getCompressionLevel()));
      pipeline.addLast(new HttpServerUpgradeHandler(httpCodec, protocol -> {
         if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
            return new Http2ServerUpgradeCodec(getHttp11To2ConnectionHandler());
         } else {
            // if we don't understand the protocol, we don't want to upgrade
            return null;
         }
      }));

      pipeline.addLast(new HttpObjectAggregator(maxContentLength()));
      pipeline.addLast("rest-handler", getHttp1Handler());
   }

   private int maxContentLength() {
      return restServer.getConfiguration().maxContentLength() + MAX_INITIAL_LINE_SIZE + MAX_HEADER_SIZE;
   }

   /**
    * Creates a handler for HTTP/1.1 -> HTTP/2 upgrade
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

   /**
    * Gets HTTP/1.1 handler.
    *
    * @return HTTP/1.1 handler.
    */
   public Http11RequestHandler getHttp1Handler() {
      return new Http11RequestHandler(restServer);
   }

   /**
    * Gets HTTP/2 handler.
    *
    * @return HTTP/2 handler.
    */
   private Http20RequestHandler getHttp2Handler() {
      return new Http20RequestHandler(restServer);
   }
}
