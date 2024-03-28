package org.infinispan.embedded;

import static org.infinispan.commons.util.concurrent.CompletableFutures.uncheckedAwait;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.LockConfiguration;
import org.infinispan.api.sync.SyncLock;
import org.infinispan.api.sync.SyncLocks;
import org.infinispan.lock.EmbeddedClusteredLockManagerFactory;
import org.infinispan.lock.api.ClusteredLockManager;

/**
 * @since 15.0
 */
public class EmbeddedSyncLocks implements SyncLocks {
   private final Embedded embedded;
   private final ClusteredLockManager lockManager;

   EmbeddedSyncLocks(Embedded embedded) {
      this.embedded = embedded;
      this.lockManager = EmbeddedClusteredLockManagerFactory.from(embedded.cacheManager);
   }

   @Override
   public SyncLock create(String name, LockConfiguration configuration) {
      return null;
   }

   @Override
   public SyncLock get(String name) {
      return new EmbeddedSyncLock(embedded, name, lockManager.get(name));
   }

   @Override
   public void remove(String name) {
      uncheckedAwait(lockManager.remove(name));
   }

   @Override
   public CloseableIterable<String> names() {
      return null;
   }
}
