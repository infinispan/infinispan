package org.infinispan.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.infinispan.api.common.events.counter.CounterEvent;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.sync.SyncStrongCounter;

/**
 * @since 14.0
 **/
public class HotRodSyncStrongCounter implements SyncStrongCounter {
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
   public HotRodSyncContainer container() {
      return hotrod.sync();
   }

   @Override
   public long value() {
      return 0;
   }

   @Override
   public long addAndGet(long delta) {
      return 0;
   }

   @Override
   public CompletableFuture<Void> reset() {
      return null;
   }

   @Override
   public AutoCloseable listen(Consumer<CounterEvent> listener) {
      return null;
   }

   @Override
   public long compareAndSwap(long expect, long update) {
      return 0;
   }

   @Override
   public CounterConfiguration configuration() {
      return null;
   }
}
