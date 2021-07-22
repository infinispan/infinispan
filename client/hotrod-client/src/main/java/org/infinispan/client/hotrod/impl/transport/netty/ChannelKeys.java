package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

public class ChannelKeys {
   private static AttributeKey<SocketAddress> UNRESOLVED_ADDRESS_KEY = AttributeKey.newInstance("unresolvedAddress");
   private static AttributeKey<CompletableFuture<Channel>> ACTIVATION_KEY = AttributeKey.newInstance("activation");
   private static AttributeKey<ChannelRecord> CHANNEL_RECORD_KEY = AttributeKey.newInstance("channelRecord");

   public static SocketAddress getUnresolvedAddress(Channel channel) {
      return channel.attr(UNRESOLVED_ADDRESS_KEY).get();
   }

   public static CompletableFuture<Channel> getActivationListener(Channel channel) {
      return channel.attr(ACTIVATION_KEY).get();
   }

   public static ChannelRecord getChannelRecord(Channel channel) {
      return channel.attr(CHANNEL_RECORD_KEY).get();
   }

   public static void init(Channel channel, ChannelOperationHandler channelOperationHandler) {
      channel.attr(CHANNEL_RECORD_KEY).set(new ChannelRecord());
      channel.attr(ACTIVATION_KEY).set(new CompletableFuture<>());
      channel.attr(UNRESOLVED_ADDRESS_KEY).set(channelOperationHandler.getUnresolvedAddress());
   }
}
