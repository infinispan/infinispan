package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultEmbeddedCacheManager;

/**
 * GetDefinedCacheCountAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetDefinedCacheCountAction extends AbstractDefaultEmbeddedCacheManagerAction<String> {

   public GetDefinedCacheCountAction(DefaultEmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      return cacheManager.getDefinedCacheCount();
   }

}
