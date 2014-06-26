package org.infinispan.server.infinispan.actions;

import org.infinispan.AdvancedCache;

/**
 * StartCacheAction.
 *
 * @author wburns
 * @since 7.0
 */
public class StartCacheAction extends AbstractAdvancedCacheAction<Void> {
   public StartCacheAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public Void run() {
      cache.start();
      return null;
   }
}
