package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * This class handles storage for unresolved address.
 */
public class ChannelRecord {
   static AttributeKey<SocketAddress> KEY = AttributeKey.newInstance("unresolved-address");

   public static SocketAddress of(Channel channel) {
      if (channel == null) {
         return null;
      }
      return channel.attr(KEY).get();
   }

   public static void set(Channel channel, SocketAddress unresolvedAddress) {
      assert !(unresolvedAddress instanceof InetSocketAddress) || ((InetSocketAddress) unresolvedAddress).isUnresolved();
      channel.attr(KEY).set(unresolvedAddress);
   }
}
