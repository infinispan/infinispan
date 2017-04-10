package org.infinispan.rest.server;

import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyTransport;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class RestChannelInitializer extends NettyChannelInitializer {

   private RestServer restServer;

   public RestChannelInitializer(RestServer server, NettyTransport transport) {
      super(server, transport, null, null);
      restServer = server;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      super.initializeChannel(ch);
      ch.pipeline().addLast(new HttpRequestDecoder());
      ch.pipeline().addLast(new HttpResponseEncoder());
      ch.pipeline().addLast(new HttpObjectAggregator(1024*100));
      ch.pipeline().addLast("rest-handler", new Http10RequestHandler(restServer.getConfiguration(), restServer.getCacheManager(), restServer.getAuthenticator()));
   }
}
