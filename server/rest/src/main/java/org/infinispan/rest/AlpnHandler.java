package org.infinispan.rest;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;

/**
 * Handler responsible for TLS/ALPN negotiation
 *
 * @author Sebastian Łaskawiec
 */
@ChannelHandler.Sharable
public class AlpnHandler extends ApplicationProtocolNegotiationHandler {

   private static final int MAX_PAYLOAD_SIZE = 5 * 1024 * 1024;

   private final RestServer restServer;

   public AlpnHandler(RestServer restServer) {
      super(ApplicationProtocolNames.HTTP_1_1);
      this.restServer = restServer;
   }

   @Override
   protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
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

   private void configureHttp2(ChannelPipeline pipeline) {
      DefaultHttp2Connection connection = new DefaultHttp2Connection(true);
      InboundHttp2ToHttpAdapter listener = new InboundHttp2ToHttpAdapterBuilder(connection)
            .propagateSettings(true).validateHttpHeaders(false)
            .maxContentLength(MAX_PAYLOAD_SIZE).build();

      pipeline.addLast(new HttpToHttp2ConnectionHandlerBuilder()
            .frameListener(listener)
            .connection(connection).build());

      pipeline.addLast("rest-handler", getHttp2Handler());
   }

   private void configureHttp1(ChannelPipeline pipeline) {
      pipeline.addLast(new HttpRequestDecoder());
      pipeline.addLast(new HttpResponseEncoder());
      pipeline.addLast(new HttpObjectAggregator(MAX_PAYLOAD_SIZE));
      pipeline.addLast("rest-handler", getHttp1Handler());
   }

   /**
    * Gets HTTP/1.1 handler.
    *
    * @return HTTP/1.1 handler.
    */
   public Http11RequestHandler getHttp1Handler() {
      return new Http11RequestHandler(restServer.getConfiguration(), restServer.getCacheManager(), restServer.getAuthenticator());
   }

   /**
    * Gets HTTP/2 handler.
    *
    * @return HTTP/2 handler.
    */
   public Http20RequestHandler getHttp2Handler() {
      return new Http20RequestHandler(restServer.getConfiguration(), restServer.getCacheManager(), restServer.getAuthenticator());
   }
}
