package org.infinispan.security.actions;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;

/**
 * GetCacheManagerCoordinatorAddress.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheManagerCoordinatorAddress extends AbstractEmbeddedCacheManagerAction<Address> {

   public GetCacheManagerCoordinatorAddress(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public Address run() {
      return cacheManager.getCoordinator();
   }
}
