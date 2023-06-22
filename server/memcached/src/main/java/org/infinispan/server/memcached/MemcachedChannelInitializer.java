package org.infinispan.server.memcached;

import org.infinispan.server.core.transport.NettyInitializer;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;

public class MemcachedChannelInitializer implements NettyInitializer {

   private final MemcachedServer memcachedServer;
   private final MemcachedProtocol protocol;

   public MemcachedChannelInitializer(MemcachedServer memcachedServer) {
      this(memcachedServer, null);
   }

   public MemcachedChannelInitializer(MemcachedServer server, MemcachedProtocol protocol) {
      this.memcachedServer = server;
      this.protocol = protocol;
   }

   @Override
   public void initializeChannel(Channel ch) {
      ChannelInboundHandler cih = protocol == null
            ? memcachedServer.getDecoder()
            : memcachedServer.getDecoder(protocol);

      ch.pipeline().addLast("decoder", cih);
      if (cih instanceof MemcachedBaseDecoder) {
         MemcachedBaseDecoder decoder = (MemcachedBaseDecoder) cih;
         memcachedServer.installMemcachedInboundHandler(ch, decoder);
      }
   }
}
