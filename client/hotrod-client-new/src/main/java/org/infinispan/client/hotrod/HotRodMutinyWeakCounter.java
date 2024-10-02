package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.mutiny.MutinyWeakCounter;

import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodMutinyWeakCounter implements MutinyWeakCounter {
   private final HotRod hotrod;
   private final String name;

   HotRodMutinyWeakCounter(HotRod hotrod, String name) {
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
   public Uni<Void> add(long delta) {
      throw new UnsupportedOperationException();
   }
}
