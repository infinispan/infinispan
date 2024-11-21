package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.CodecHolder;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.handler.codec.FixedLengthFrameDecoder;

public class TestChannelFactory extends ChannelFactory {
   public TestChannelFactory(Configuration configuration, CodecHolder codecHolder) {
      super(configuration, codecHolder);
   }

   @Override
   public ChannelInitializer createChannelInitializer(SocketAddress address, Bootstrap bootstrap) {
      return new ChannelInitializer(bootstrap, address, getOperationsFactory(), getConfiguration(), this, null, null) {
         @Override
         protected void initChannel(Channel channel) throws Exception {
            super.initChannel(channel);
            channel.pipeline().addFirst("1frame", new FixedLengthFrameDecoder(1));
         }
      };
   }
}
