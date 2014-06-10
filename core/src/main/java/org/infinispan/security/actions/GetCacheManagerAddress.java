package org.infinispan.security.actions;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;

/**
 * GetCacheManagerAddress.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheManagerAddress extends AbstractEmbeddedCacheManagerAction<Address> {

   public GetCacheManagerAddress(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public Address run() {
      return cacheManager.getAddress();
   }
}
