package org.infinispan.security.actions;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetCacheManagerConfigurationAction.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class GetCacheConfigurationFromManagerAction extends AbstractEmbeddedCacheManagerAction<Configuration> {
   private final String name;

   public GetCacheConfigurationFromManagerAction(EmbeddedCacheManager cacheManager, String name) {
      super(cacheManager);
      this.name = name;
   }

   @Override
   public Configuration get() {
      return cacheManager.getCacheConfiguration(name);
   }

}
