package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultCacheContainer;

/**
 * GetRunningCacheCountAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetRunningCacheCountAction extends AbstractDefaultCacheContainerAction<String> {

   public GetRunningCacheCountAction(DefaultCacheContainer cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      return cacheManager.getRunningCacheCount();
   }

}
