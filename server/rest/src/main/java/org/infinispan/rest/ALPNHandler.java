package org.infinispan.rest;

import static org.infinispan.rest.RestChannelInitializer.MAX_HEADER_SIZE;
import static org.infinispan.rest.RestChannelInitializer.MAX_INITIAL_LINE_SIZE;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.server.core.ProtocolServer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler.UpgradeCodecFactory;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http2.CleartextHttp2ServerUpgradeHandler;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2MultiplexCodec;
import io.netty.handler.codec.http2.Http2MultiplexCodecBuilder;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
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

   protected final RestServer restServer;
   volatile List<CorsConfig> corsRules;

   public ALPNHandler(RestServer restServer) {
      super(ApplicationProtocolNames.HTTP_1_1);
      this.restServer = restServer;
   }

   @Override
   public void configurePipeline(ChannelHandlerContext ctx, String protocol) {
      configurePipeline(ctx.pipeline(), protocol);
   }

   public void configurePipeline(ChannelPipeline pipeline, String protocol) {
      if (ApplicationProtocolNames.HTTP_2.equals(protocol) || ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
         configureHttpPipeline(pipeline);
         return;
      }

      ProtocolServer<?> protocolServer = getProtocolServer(protocol);
      if (protocolServer != null) {
         pipeline.addLast(protocolServer.getInitializer());
         return;
      }

      throw new IllegalStateException("unknown protocol: " + protocol);
   }

   protected ProtocolServer<?> getProtocolServer(String protocol) {
      return null;
   }

   /**
    * Configure the handlers that should be used for both HTTP 1.1 and HTTP 2.0
    */
   private void addCommonsHandlers(ChannelPipeline pipeline) {
      // Handles http content encoding (gzip)
      pipeline.addLast(new HttpContentCompressor(restServer.getConfiguration().getCompressionLevel()));
      // Handles chunked data
      pipeline.addLast(new HttpObjectAggregator(maxContentLength()));
      // Handles Http/2 headers propagation from request to response
      pipeline.addLast(new StreamCorrelatorHandler());
      // Handles CORS
      pipeline.addLast(new CorsHandler(getCorsConfigs(), true));
      // Handles Keep-alive
      pipeline.addLast(new HttpServerKeepAliveHandler());
      // Handles the writing of ChunkedInputs
      pipeline.addLast(new ChunkedWriteHandler());
      // Handles REST request
      pipeline.addLast(new RestRequestHandler(restServer));
   }

   private List<CorsConfig> getCorsConfigs() {
      List<CorsConfig> rules = corsRules;
      if (rules == null) {
         synchronized (this) {
            rules = corsRules;
            if (rules == null) {
               rules = new ArrayList<>();
               rules.addAll(CorsUtil.enableAllForSystemConfig());
               rules.addAll(CorsUtil.enableAllForLocalHost(restServer.getPort(), CROSS_ORIGIN_ALT_PORT));
               rules.addAll(restServer.getConfiguration().getCorsRules());
               corsRules = rules;
            }
         }
      }
      return corsRules;
   }

   protected void configureHttpPipeline(ChannelPipeline pipeline) {
      //TODO [ISPN-12082]: Rework pipeline removing deprecated codecs
      Http2MultiplexCodec multiplexCodec = Http2MultiplexCodecBuilder.forServer(new ChannelInitializer<Channel>() {
         @Override
         protected void initChannel(Channel channel) {
            // Creates the HTTP/2 pipeline, where each stream is handled by a sub-channel.
            ChannelPipeline p = channel.pipeline();
            p.addLast(new Http2StreamFrameToHttpObjectCodec(true));
            addCommonsHandlers(p);
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
      HttpServerCodec httpCodec = new HttpServerCodec(MAX_INITIAL_LINE_SIZE, MAX_HEADER_SIZE, maxContentLength());
      HttpServerUpgradeHandler upgradeHandler = new HttpServerUpgradeHandler(httpCodec, upgradeCodecFactory, maxContentLength());
      CleartextHttp2ServerUpgradeHandler cleartextHttp2ServerUpgradeHandler = new CleartextHttp2ServerUpgradeHandler(httpCodec, upgradeHandler, multiplexCodec);
      pipeline.addLast(cleartextHttp2ServerUpgradeHandler);

      addCommonsHandlers(pipeline);
   }

   protected int maxContentLength() {
      return this.restServer.getConfiguration().maxContentLength() + MAX_INITIAL_LINE_SIZE + MAX_HEADER_SIZE;
   }

   public ChannelHandler getRestHandler() {
      return new RestRequestHandler(restServer);
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
