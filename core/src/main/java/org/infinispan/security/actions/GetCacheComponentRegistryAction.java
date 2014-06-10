package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.factories.ComponentRegistry;

/**
 * GetCacheComponentRegistryAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheComponentRegistryAction extends AbstractAdvancedCacheAction<ComponentRegistry> {

   public GetCacheComponentRegistryAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public ComponentRegistry run() {
      return cache.getComponentRegistry();
   }

}
