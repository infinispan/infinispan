package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;

/**
 * GetCacheConfigurationAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheConfigurationAction extends AbstractAdvancedCacheAction<Configuration> {

   public GetCacheConfigurationAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public Configuration run() {
      return cache.getCacheConfiguration();
   }

}
