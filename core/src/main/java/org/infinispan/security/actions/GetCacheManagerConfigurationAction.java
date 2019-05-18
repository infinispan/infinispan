package org.infinispan.security.actions;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetCacheManagerConfigurationAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheManagerConfigurationAction extends AbstractEmbeddedCacheManagerAction<GlobalConfiguration> {

   public GetCacheManagerConfigurationAction(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public GlobalConfiguration run() {
      return cacheManager.getCacheManagerConfiguration();
   }

}
