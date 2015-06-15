package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultCacheContainer;

/**
 * GetCreatedCacheCountAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetCreatedCacheCountAction extends AbstractDefaultCacheContainerAction<String> {

   public GetCreatedCacheCountAction(DefaultCacheContainer cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      return cacheManager.getCreatedCacheCount();
   }

}
