package org.infinispan.server.infinispan.actions;

import org.infinispan.AdvancedCache;

/**
 * ClearCacheAction.
 *
 * @author wburns
 * @since 7.0
 */
public class ClearCacheAction extends AbstractAdvancedCacheAction<Void> {
   public ClearCacheAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public Void run() {
      cache.clear();
      return null;
   }
}
