package org.infinispan.hotrod;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.LockConfiguration;
import org.infinispan.api.sync.SyncLocks;

/**
 * @since 14.0
 **/
public class HotRodSyncLocks implements SyncLocks {
   private final HotRod hotrod;

   HotRodSyncLocks(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public HotRodSyncLock create(String name, LockConfiguration configuration) {
      return new HotRodSyncLock(hotrod, name); // PLACEHOLDER
   }

   @Override
   public HotRodSyncLock get(String name) {
      return new HotRodSyncLock(hotrod, name); // PLACEHOLDER
   }

   @Override
   public void remove(String name) {

   }

   @Override
   public CloseableIterable<String> names() {
      return null;
   }
}
