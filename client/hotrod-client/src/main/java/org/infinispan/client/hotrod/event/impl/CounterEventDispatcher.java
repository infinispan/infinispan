package org.infinispan.client.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class CounterEventDispatcher extends EventDispatcher<HotRodCounterEvent> {
   private final ConcurrentMap<String, List<Consumer<HotRodCounterEvent>>> clientListeners;
   private final Supplier<CompletableFuture<Short>> failover;

   public CounterEventDispatcher(ClientListenerNotifier notifier, byte[] listenerId,
                                 ConcurrentMap<String, List<Consumer<HotRodCounterEvent>>> clientListeners,
                                 Channel channel, Supplier<CompletableFuture<Short>> failover) {
      super(notifier, "", null, listenerId, channel);
      this.clientListeners = clientListeners;
      this.failover = failover;
   }

   @Override
   public CompletableFuture<Short> executeFailover() {
      return failover.get();
   }

   @Override
   protected HotRodCounterEvent readEvent(ByteBuf buf) {
      return notifier.codec().readCounterEvent(buf, listenerId);
   }

   @Override
   protected void invokeEvent(HotRodCounterEvent event) {
      if (trace) {
         log.tracef("Received counter event %s", event);
      }
      clientListeners.getOrDefault(event.getCounterName(), Collections.emptyList())
            .parallelStream()
            .forEach(handle -> handle.accept(event));
   }


   public SocketAddress address() {
      return ChannelRecord.of(channel).getUnresolvedAddress();
   }

   public void close() {
      channel.close();
   }
}
