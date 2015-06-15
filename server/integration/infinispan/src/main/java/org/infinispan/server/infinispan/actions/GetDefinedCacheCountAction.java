package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultCacheContainer;

/**
 * GetDefinedCacheCountAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetDefinedCacheCountAction extends AbstractDefaultCacheContainerAction<String> {

   public GetDefinedCacheCountAction(DefaultCacheContainer cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      return cacheManager.getDefinedCacheCount();
   }

}
