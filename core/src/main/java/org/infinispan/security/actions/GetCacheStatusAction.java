package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * GetCacheStatusAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheStatusAction extends AbstractAdvancedCacheAction<ComponentStatus> {

   public GetCacheStatusAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public ComponentStatus run() {
      return cache.getStatus();
   }

}
