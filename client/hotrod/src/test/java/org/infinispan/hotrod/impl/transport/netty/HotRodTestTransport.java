package org.infinispan.hotrod.impl.transport.netty;

import java.net.SocketAddress;

import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.impl.HotRodTransport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.handler.codec.FixedLengthFrameDecoder;

public class HotRodTestTransport {

   public static HotRodTransport createTestTransport(HotRodConfiguration configuration) {
      return new HotRodTransport(configuration) {
         @Override
         protected ChannelFactory createChannelFactory() {
            return new ChannelFactory() {
               @Override
               protected ChannelInitializer createChannelInitializer(SocketAddress address,
                                                                     Bootstrap bootstrap) {
                  return new ChannelInitializer(bootstrap, address, getCacheOperationsFactory(), getConfiguration(), this, topologyInfo.getCluster()) {
                     @Override
                     protected void initChannel(Channel channel) throws Exception {
                        super.initChannel(channel);
                        channel.pipeline().addFirst("1frame", new FixedLengthFrameDecoder(1));
                     }
                  };
               }
            };
         }
      };
   }
}
