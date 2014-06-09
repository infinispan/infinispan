package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultEmbeddedCacheManager;

/**
 * GetCreatedCacheCountAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetCreatedCacheCountAction extends AbstractDefaultEmbeddedCacheManagerAction<String> {

   public GetCreatedCacheCountAction(DefaultEmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      return cacheManager.getCreatedCacheCount();
   }

}
