package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultCacheContainer;

/**
 * GetDefinedCacheNamesAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetDefinedCacheNamesAction extends AbstractDefaultCacheContainerAction<String> {

   public GetDefinedCacheNamesAction(DefaultCacheContainer cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      return cacheManager.getDefinedCacheNames();
   }

}
