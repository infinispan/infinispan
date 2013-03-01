package org.infinispan.security.actions;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetGlobalComponentRegistryAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetGlobalComponentRegistryAction extends AbstractEmbeddedCacheManagerAction<GlobalComponentRegistry> {

   public GetGlobalComponentRegistryAction(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public GlobalComponentRegistry run() {
      return cacheManager.getGlobalComponentRegistry();
   }

}
