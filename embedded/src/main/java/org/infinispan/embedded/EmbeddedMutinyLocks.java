package org.infinispan.embedded;

import org.infinispan.api.configuration.LockConfiguration;
import org.infinispan.api.mutiny.MutinyLock;
import org.infinispan.api.mutiny.MutinyLocks;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 15.0
 */
public class EmbeddedMutinyLocks implements MutinyLocks {
   private final Embedded embedded;

   EmbeddedMutinyLocks(Embedded embedded) {
      this.embedded = embedded;
   }

   @Override
   public Uni<MutinyLock> lock(String name) {
      return null;
   }

   @Override
   public Uni<MutinyLock> create(String name, LockConfiguration configuration) {
      return null;
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
