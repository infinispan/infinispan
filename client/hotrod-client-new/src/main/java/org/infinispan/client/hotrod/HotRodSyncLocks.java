package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.LockConfiguration;
import org.infinispan.api.sync.SyncLock;
import org.infinispan.api.sync.SyncLocks;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncLocks implements SyncLocks {
   private final HotRod hotrod;

   HotRodSyncLocks(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public SyncLock create(String name, LockConfiguration configuration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public SyncLock get(String name) {
      return new HotRodSyncLock(hotrod, name); // PLACEHOLDER
   }

   @Override
   public void remove(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CloseableIterable<String> names() {
      throw new UnsupportedOperationException();
   }
}
