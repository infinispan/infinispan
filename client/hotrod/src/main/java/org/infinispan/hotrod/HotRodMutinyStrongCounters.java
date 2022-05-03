package org.infinispan.hotrod;

import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.mutiny.MutinyStrongCounter;
import org.infinispan.api.mutiny.MutinyStrongCounters;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyStrongCounters implements MutinyStrongCounters {
   private final HotRod hotrod;

   HotRodMutinyStrongCounters(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public Uni<MutinyStrongCounter> get(String name) {
      return Uni.createFrom().item(new HotRodMutinyStrongCounter(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public Uni<MutinyStrongCounter> create(String name, CounterConfiguration configuration) {
      return Uni.createFrom().item(new HotRodMutinyStrongCounter(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public Uni<Void> remove(String name) {
      return null;
   }

   @Override
   public Multi<String> names() {
      return null;
   }
}
