package org.infinispan.client.hotrod.impl.transport.netty.pool;

import io.netty.channel.pool.ChannelPool;

public interface HotRodChannelPool extends ChannelPool {

   int getCreated();

   int getActive();

   int getIdle();
}
