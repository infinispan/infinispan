package org.infinispan.hotrod;

import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.mutiny.MutinyWeakCounter;
import org.infinispan.api.mutiny.MutinyWeakCounters;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyWeakCounters implements MutinyWeakCounters {
   private final HotRod hotrod;

   HotRodMutinyWeakCounters(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public Uni<MutinyWeakCounter> get(String name) {
      return Uni.createFrom().item(new HotRodMutinyWeakCounter(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public Uni<MutinyWeakCounter> create(String name, CounterConfiguration configuration) {
      return Uni.createFrom().item(new HotRodMutinyWeakCounter(hotrod, name)); // PLACEHOLDER
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
