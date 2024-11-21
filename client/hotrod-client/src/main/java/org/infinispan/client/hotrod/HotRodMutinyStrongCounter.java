package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.events.counter.CounterEvent;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.mutiny.MutinyStrongCounter;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodMutinyStrongCounter implements MutinyStrongCounter {
   private final HotRod hotrod;
   private final String name;

   HotRodMutinyStrongCounter(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public MutinyContainer container() {
      return hotrod.mutiny();
   }

   @Override
   public Uni<Long> value() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Long> addAndGet(long delta) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Void> reset() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<CounterEvent> listen() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> compareAndSet(long expect, long update) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Long> compareAndSwap(long expect, long update) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Long> getAndSet(long value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<CounterConfiguration> getConfiguration() {
      throw new UnsupportedOperationException();
   }
}
