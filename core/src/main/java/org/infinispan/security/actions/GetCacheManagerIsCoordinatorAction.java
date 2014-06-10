package org.infinispan.security.actions;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetCacheManagerIsCoordinatorAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheManagerIsCoordinatorAction extends AbstractEmbeddedCacheManagerAction<Boolean> {

   public GetCacheManagerIsCoordinatorAction(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public Boolean run() {
      return cacheManager.isCoordinator();
   }
}
