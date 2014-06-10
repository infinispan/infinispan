package org.infinispan.security.actions;

import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetCacheManagerStatusAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheManagerStatusAction extends AbstractEmbeddedCacheManagerAction<ComponentStatus> {

   public GetCacheManagerStatusAction(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public ComponentStatus run() {
      return cacheManager.getStatus();
   }

}
