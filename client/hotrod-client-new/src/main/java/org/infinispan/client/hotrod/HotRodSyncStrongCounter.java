package org.infinispan.client.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.events.counter.CounterEvent;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncStrongCounter;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncStrongCounter implements SyncStrongCounter {
   private final HotRod hotrod;
   private final String name;

   HotRodSyncStrongCounter(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public SyncContainer container() {
      return hotrod.sync();
   }

   @Override
   public long value() {
      throw new UnsupportedOperationException();
   }

   @Override
   public long addAndGet(long delta) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> reset() {
      throw new UnsupportedOperationException();
   }

   @Override
   public AutoCloseable listen(Consumer<CounterEvent> listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public long compareAndSwap(long expect, long update) {
      throw new UnsupportedOperationException();
   }

   @Override
   public long getAndSet(long value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CounterConfiguration configuration() {
      throw new UnsupportedOperationException();
   }
}
