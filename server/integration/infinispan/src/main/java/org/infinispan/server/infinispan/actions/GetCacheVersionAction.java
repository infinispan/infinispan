package org.infinispan.server.infinispan.actions;

import org.infinispan.AdvancedCache;

/**
 * GetCacheVersionAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetCacheVersionAction extends AbstractAdvancedCacheAction<String> {

   public GetCacheVersionAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public String run() {
      return cache.getVersion();
   }

}
