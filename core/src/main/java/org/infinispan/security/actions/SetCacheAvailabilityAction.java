package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.partionhandling.AvailabilityMode;

/**
 * SetCacheAvailabilityAction.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class SetCacheAvailabilityAction extends AbstractAdvancedCacheAction<Void> {

   private AvailabilityMode availabilityMode;

   public SetCacheAvailabilityAction(AdvancedCache<?, ?> cache, AvailabilityMode availabilityMode) {
      super(cache);
      this.availabilityMode = availabilityMode;
   }

   @Override
   public Void run() {
      cache.setAvailability(availabilityMode);
      return null;
   }

}
