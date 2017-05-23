package org.infinispan.server.router.router.impl.rest.handlers;

import org.infinispan.server.router.RoutingTable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

/**
 * Initializer for REST Handlers.
 *
 * @author Sebastian Łaskawiec
 */
public class ChannelInboundHandlerDelegatorInitializer extends ChannelInitializer<Channel> {

   private final RoutingTable routingTable;

   public ChannelInboundHandlerDelegatorInitializer(RoutingTable routingTable) {
      this.routingTable = routingTable;
   }

   @Override
   protected void initChannel(Channel channel) throws Exception {
      channel.pipeline().addLast(new HttpRequestDecoder());
      channel.pipeline().addLast(new HttpResponseEncoder());
      channel.pipeline().addLast(new HttpObjectAggregator(1024*100));
      channel.pipeline().addLast(new ChannelInboundHandlerDelegator(routingTable));
   }
}
