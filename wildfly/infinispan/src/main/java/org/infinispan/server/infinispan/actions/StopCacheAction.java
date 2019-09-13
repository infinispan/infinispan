package org.infinispan.server.infinispan.actions;

import org.infinispan.AdvancedCache;

/**
 * StopCacheAction.
 *
 * @author wburns
 * @since 7.0
 */
public class StopCacheAction extends AbstractAdvancedCacheAction<Void> {
   public StopCacheAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public Void run() {
      cache.stop();
      return null;
   }
}
