package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultEmbeddedCacheManager;

/**
 * GetDefinedCacheNamesAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetDefinedCacheNamesAction extends AbstractDefaultEmbeddedCacheManagerAction<String> {

   public GetDefinedCacheNamesAction(DefaultEmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      return cacheManager.getDefinedCacheNames();
   }

}
