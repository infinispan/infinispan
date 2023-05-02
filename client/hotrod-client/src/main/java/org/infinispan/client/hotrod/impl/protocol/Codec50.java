package org.infinispan.client.hotrod.impl.protocol;

import java.net.SocketAddress;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelInitializer;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelPool;
import org.infinispan.client.hotrod.impl.transport.netty.V5ChannelPool;

import io.netty.util.concurrent.EventExecutor;

/**
 * @since 15.0
 */
public class Codec50 extends Codec40 {
   public ChannelPool createPool(EventExecutor executor, SocketAddress address, ChannelInitializer channelInitializer,
                                 BiConsumer<ChannelPool, ChannelFactory.ChannelEventType> connectionFailureListener,
                                 Configuration configuration) {
      return V5ChannelPool.createAndStartPool(address, channelInitializer, connectionFailureListener);
   }
}
