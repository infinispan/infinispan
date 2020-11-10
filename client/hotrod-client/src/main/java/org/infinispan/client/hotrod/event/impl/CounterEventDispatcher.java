package org.infinispan.client.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;

public class CounterEventDispatcher extends EventDispatcher<HotRodCounterEvent> {
   private final ConcurrentMap<String, List<Consumer<HotRodCounterEvent>>> clientListeners;
   private final Supplier<CompletableFuture<Short>> failover;

   public CounterEventDispatcher(byte[] listenerId,
                                 ConcurrentMap<String, List<Consumer<HotRodCounterEvent>>> clientListeners,
                                 SocketAddress address, Supplier<CompletableFuture<Short>> failover, Runnable cleanup) {
      super("", null, listenerId, address, cleanup);
      this.clientListeners = clientListeners;
      this.failover = failover;
   }

   @Override
   public CompletableFuture<Short> executeFailover() {
      return failover.get();
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
}
