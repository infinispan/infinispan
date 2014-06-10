package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * GetCacheLockManagerAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheLockManagerAction extends AbstractAdvancedCacheAction<LockManager> {

   public GetCacheLockManagerAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public LockManager run() {
      return cache.getLockManager();
   }

}
