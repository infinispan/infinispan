package org.infinispan.server.infinispan.actions;

import org.infinispan.remoting.transport.Address;
import org.jboss.as.clustering.infinispan.DefaultEmbeddedCacheManager;

import java.util.List;

/**
 * GetMembersAction.
 *
 * @author William Burns
 * @since 7.0
 */

public class GetMembersAction extends AbstractDefaultEmbeddedCacheManagerAction<List<Address>> {

   public GetMembersAction(DefaultEmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public List<Address> run() {
      return cacheManager.getMembers();
   }

}
