package org.infinispan.rest;

import static org.infinispan.rest.RestChannelInitializer.MAX_HEADER_SIZE;
import static org.infinispan.rest.RestChannelInitializer.MAX_INITIAL_LINE_SIZE;

import java.util.Collections;
import java.util.Map;

import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.transport.AccessControlFilter;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler.UpgradeCodecFactory;
import io.netty.handler.codec.http2.CleartextHttp2ServerUpgradeHandler;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2MultiplexCodec;
import io.netty.handler.codec.http2.Http2MultiplexCodecBuilder;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.AsciiString;

/**
 * Handler responsible for TLS/ALPN negotiation.
 *
 * @author Sebastian Łaskawiec
 */
public class ALPNHandler extends ApplicationProtocolNegotiationHandler {

   protected final RestServer restServer;

   public ALPNHandler(RestServer restServer) {
      super(ApplicationProtocolNames.HTTP_1_1);
      this.restServer = restServer;
   }

   @Override
   public void configurePipeline(ChannelHandlerContext ctx, String protocol) {
      configurePipeline(ctx.pipeline(), protocol, restServer, Collections.emptyMap());
   }

   public static void configurePipeline(ChannelPipeline pipeline, String protocol, RestServer restServer, Map<String, ProtocolServer<?>> upgradeServers) {
      if (ApplicationProtocolNames.HTTP_2.equals(protocol) || ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
         configureHttpPipeline(pipeline, restServer);
         return;
      }

      ProtocolServer<?> protocolServer = upgradeServers.get(protocol);
      if (protocolServer != null) {
         pipeline.addLast(protocolServer.getInitializer());
         return;
      }

      throw new IllegalStateException("unknown protocol: " + protocol);
   }

   /**
    * Configure the handlers that should be used for both HTTP 1.1 and HTTP 2.0
    */
   private static void addCommonHandlers(ChannelPipeline pipeline, RestServer restServer) {
      // Handles IP filtering for the HTTP connector
      RestServerConfiguration restServerConfiguration = restServer.getConfiguration();
      pipeline.addLast(new AccessControlFilter<>(restServerConfiguration, false));
      // Handles http content encoding (gzip)
      pipeline.addLast(new HttpContentCompressor(restServerConfiguration.getCompressionLevel()));
      // Handles chunked data
      pipeline.addLast(new HttpObjectAggregator(restServer.maxContentLength()));
      // Handles Http/2 headers propagation from request to response
      pipeline.addLast(new StreamCorrelatorHandler());
      // Handles CORS
      pipeline.addLast(new CorsHandler(restServer.getCorsConfigs(), true));
      // Handles Keep-alive
      pipeline.addLast(new HttpServerKeepAliveHandler());
      // Handles the writing of ChunkedInputs
      pipeline.addLast(new ChunkedWriteHandler());
      // Handles REST request
      pipeline.addLast(new RestRequestHandler(restServer));
   }

   private static void configureHttpPipeline(ChannelPipeline pipeline, RestServer restServer) {
      //TODO [ISPN-12082]: Rework pipeline removing deprecated codecs
      Http2MultiplexCodec multiplexCodec = Http2MultiplexCodecBuilder.forServer(new ChannelInitializer<>() {
         @Override
         protected void initChannel(Channel channel) {
            // Creates the HTTP/2 pipeline, where each stream is handled by a sub-channel.
            ChannelPipeline p = channel.pipeline();
            p.addLast(new Http2StreamFrameToHttpObjectCodec(true));
            addCommonHandlers(p, restServer);
         }
      }).initialSettings(Http2Settings.defaultSettings()).build();

      UpgradeCodecFactory upgradeCodecFactory = protocol -> {
         if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
            return new Http2ServerUpgradeCodec(multiplexCodec);
         } else {
            return null;
         }
      };
      // handler for clear-text upgrades
      HttpServerCodec httpCodec = new HttpServerCodec(MAX_INITIAL_LINE_SIZE, MAX_HEADER_SIZE, restServer.maxContentLength());
      HttpServerUpgradeHandler upgradeHandler = new HttpServerUpgradeHandler(httpCodec, upgradeCodecFactory, restServer.maxContentLength());
      CleartextHttp2ServerUpgradeHandler cleartextHttp2ServerUpgradeHandler = new CleartextHttp2ServerUpgradeHandler(httpCodec, upgradeHandler, multiplexCodec);
      pipeline.addLast(cleartextHttp2ServerUpgradeHandler);

      addCommonHandlers(pipeline, restServer);
   }
}
