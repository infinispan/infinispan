package org.infinispan.hotrod;

import org.infinispan.api.configuration.LockConfiguration;
import org.infinispan.api.mutiny.MutinyLock;
import org.infinispan.api.mutiny.MutinyLocks;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyLocks implements MutinyLocks {
   private final HotRod hotrod;

   HotRodMutinyLocks(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public Uni<MutinyLock> lock(String name) {
      return Uni.createFrom().item(new HotRodMutinyLock(hotrod, name));
   }

   @Override
   public Uni<MutinyLock> create(String name, LockConfiguration configuration) {
      return Uni.createFrom().item(new HotRodMutinyLock(hotrod, name));
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
