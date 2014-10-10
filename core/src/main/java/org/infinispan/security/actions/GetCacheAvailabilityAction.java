package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.partionhandling.AvailabilityMode;

/**
 * GetCacheAvailabilityAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheAvailabilityAction extends AbstractAdvancedCacheAction<AvailabilityMode> {

   public GetCacheAvailabilityAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public AvailabilityMode run() {
      return cache.getAvailability();
   }

}
