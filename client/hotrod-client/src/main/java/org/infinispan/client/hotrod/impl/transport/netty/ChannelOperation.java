package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;

import io.netty.channel.Channel;

/**
 * A callback to be invoked on a channel.
 */
public interface ChannelOperation {
   /**
    * Invoked on an active channel ready to be written
    */
   void invoke(Channel channel);

   /**
    * Invoked when the callback cannot be invoked due to timeout or terminated pool.
    * @param address
    * @param cause
    */
   void cancel(SocketAddress address, Throwable cause);
}
