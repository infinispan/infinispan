package org.infinispan.server.core.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

import java.util.Collections;
import java.util.List;

/**
 * @author wburns
 * @since 9.0
 */
public class NettyInitializers extends ChannelInitializer<Channel> {
   private final List<? extends NettyInitializer> initializers;

   public NettyInitializers(NettyInitializer initializer) {
      this(Collections.singletonList(initializer));
   }

   public NettyInitializers(List<? extends NettyInitializer> initializers) {
      this.initializers = initializers;
   }

   @Override
   protected void initChannel(Channel ch) throws Exception {
      for (NettyInitializer ni : initializers) {
         ni.initializeChannel(ch);
      }
   }
}
