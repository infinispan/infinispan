package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultEmbeddedCacheManager;

/**
 * GetRunningCacheCountAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetRunningCacheCountAction extends AbstractDefaultEmbeddedCacheManagerAction<String> {

   public GetRunningCacheCountAction(DefaultEmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      return cacheManager.getRunningCacheCount();
   }

}
