package org.infinispan.hotrod;

import org.infinispan.api.mutiny.MutinyWeakCounter;

import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyWeakCounter implements MutinyWeakCounter {
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
   public HotRodMutinyContainer container() {
      return hotrod.mutiny();
   }

   @Override
   public Uni<Long> value() {
      return null;
   }

   @Override
   public Uni<Void> add(long delta) {
      return null;
   }
}
