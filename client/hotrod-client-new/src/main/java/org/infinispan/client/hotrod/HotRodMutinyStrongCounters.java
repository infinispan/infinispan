package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.mutiny.MutinyStrongCounter;
import org.infinispan.api.mutiny.MutinyStrongCounters;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodMutinyStrongCounters implements MutinyStrongCounters {
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
