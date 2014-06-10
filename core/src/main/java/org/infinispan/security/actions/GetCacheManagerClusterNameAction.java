package org.infinispan.security.actions;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetCacheManagerClusterNameAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheManagerClusterNameAction extends AbstractEmbeddedCacheManagerAction<String> {


   public GetCacheManagerClusterNameAction(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      return cacheManager.getClusterName();
   }

}
