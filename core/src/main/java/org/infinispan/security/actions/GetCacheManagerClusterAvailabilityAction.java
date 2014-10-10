package org.infinispan.security.actions;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.topology.LocalTopologyManagerImpl;

/**
 * GetCacheManagerClusterAvailabilityAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheManagerClusterAvailabilityAction extends AbstractEmbeddedCacheManagerAction<String> {


   public GetCacheManagerClusterAvailabilityAction(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public String run() {
      LocalTopologyManagerImpl localTopologyManager = cacheManager.getGlobalComponentRegistry().getComponent(LocalTopologyManagerImpl.class);
      return localTopologyManager != null ? localTopologyManager.getClusterAvailability() : AvailabilityMode.AVAILABLE.toString();
   }

}
