package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.mutiny.MutinyWeakCounter;
import org.infinispan.api.mutiny.MutinyWeakCounters;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodMutinyWeakCounters implements MutinyWeakCounters {
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
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Void> remove(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<String> names() {
      throw new UnsupportedOperationException();
   }
}
