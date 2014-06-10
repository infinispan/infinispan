package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.distribution.DistributionManager;

/**
 * GetCacheDistributionManagerAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheDistributionManagerAction extends AbstractAdvancedCacheAction<DistributionManager> {

   public GetCacheDistributionManagerAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public DistributionManager run() {
      return cache.getDistributionManager();
   }

}
