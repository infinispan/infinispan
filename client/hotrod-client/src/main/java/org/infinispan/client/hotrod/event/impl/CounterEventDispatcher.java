package org.infinispan.client.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;

public class CounterEventDispatcher extends EventDispatcher<HotRodCounterEvent> implements FailoverEventDispatcher {
   private volatile SocketAddress address;
   private final ConcurrentMap<String, List<Consumer<HotRodCounterEvent>>> clientListeners;
   private final Supplier<CompletableFuture<Void>> failover;

   public CounterEventDispatcher(byte[] listenerId,
                                 ConcurrentMap<String, List<Consumer<HotRodCounterEvent>>> clientListeners,
                                 SocketAddress address, Supplier<CompletableFuture<Void>> failover, Runnable cleanup) {
      super("", null, listenerId, cleanup);
      this.address = address;
      this.clientListeners = clientListeners;
      this.failover = failover;
   }

   @Override
   public synchronized void addAddress(SocketAddress address) {
      if (address != null) {
         this.address = address;
      }
   }

   @Override
   public synchronized boolean removeAddress(SocketAddress address) {
      if (address != null && Objects.equals(this.address, address)) {
         this.address = null;
         return true;
      }
      return false;
   }

   @Override
   public synchronized void replaceAddress(SocketAddress oldAddress, SocketAddress newAddress) {
      if (Objects.equals(address, oldAddress)) {
         address = newAddress;
      }
   }

   @Override
   public Set<SocketAddress> addresses() {
      SocketAddress current = address;
      return current == null ? Set.of() : Set.of(current);
   }

   public SocketAddress address() {
      return address;
   }

   @Override
   protected synchronized void clearAddresses() {
      address = null;
   }

   @Override
   public CompletableFuture<Void> executeFailover() {
      return failover.get();
   }

   @Override
   protected void invokeEvent(HotRodCounterEvent event) {
      if (log.isTraceEnabled()) {
         log.tracef("Received counter event %s", event);
      }
      clientListeners.getOrDefault(event.getCounterName(), Collections.emptyList())
            .parallelStream()
            .forEach(handle -> handle.accept(event));
   }
}
