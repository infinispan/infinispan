package org.infinispan.rest;

import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyTransport;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

/**
 * Creates Netty Channels for this server.
 *
 * @author Sebastian Łaskawiec
 */
public class RestChannelInitializer extends NettyChannelInitializer {

   private static final int MAX_PAYLOAD_SIZE = 5 * 1024 * 1024;

   private RestServer restServer;

   /**
    * Creates new {@link RestChannelInitializer}.
    *
    * @param server Rest Server this initializer belongs to.
    * @param transport Netty transport.
    */
   public RestChannelInitializer(RestServer server, NettyTransport transport) {
      super(server, transport, null, null);
      restServer = server;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      super.initializeChannel(ch);
      ch.pipeline().addLast(new HttpRequestDecoder());
      ch.pipeline().addLast(new HttpResponseEncoder());
      ch.pipeline().addLast(new HttpObjectAggregator(MAX_PAYLOAD_SIZE));
      ch.pipeline().addLast("rest-handler", getHttpHandler());
   }

   /**
    * Returns new instance of the main HTTP Handler.
    *
    * @return new instance of the main HTTP Handler.
    */
   public Http10RequestHandler getHttpHandler() {
      return new Http10RequestHandler(restServer.getConfiguration(), restServer.getCacheManager(), restServer.getAuthenticator());
   }
}
