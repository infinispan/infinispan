package org.infinispan.hotrod;

import org.infinispan.api.common.events.counter.CounterEvent;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.mutiny.MutinyStrongCounter;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyStrongCounter implements MutinyStrongCounter {
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
   public HotRodMutinyContainer container() {
      return hotrod.mutiny();
   }

   @Override
   public Uni<Long> value() {
      return null;
   }

   @Override
   public Uni<Long> addAndGet(long delta) {
      return null;
   }

   @Override
   public Uni<Void> reset() {
      return null;
   }

   @Override
   public Multi<CounterEvent> listen() {
      return null;
   }

   @Override
   public Uni<Boolean> compareAndSet(long expect, long update) {
      return null;
   }

   @Override
   public Uni<Long> compareAndSwap(long expect, long update) {
      return null;
   }

   @Override
   public Uni<Long> getAndSet(long value) {
      return null;
   }

   @Override
   public Uni<CounterConfiguration> getConfiguration() {
      return null;
   }
}
