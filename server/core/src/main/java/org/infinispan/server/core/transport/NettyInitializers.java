package org.infinispan.server.core.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

/**
 * @author wburns
 * @since 9.0
 */
public class NettyInitializers extends ChannelInitializer<Channel> {
   private final NettyInitializer[] initializers;

   public NettyInitializers(NettyInitializer... initializers) {
      this.initializers = initializers;
   }

   @Override
   protected void initChannel(Channel ch) throws Exception {
      for (NettyInitializer ni : initializers) {
         ni.initializeChannel(ch);
      }
   }
}
