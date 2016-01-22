package org.infinispan.xsite.status;

import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * A per-cache {@link SiteStatus} builder.
 * <p>
 * It builds a {@link SiteStatus} based on the number of node with the site online and offline.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class CacheSiteStatusBuilder extends AbstractSiteStatusBuilder<Address> {

   public CacheSiteStatusBuilder() {
      super();
   }

   /**
    * Adds a member with an online/offline connection to the server based on the {@code online} parameter.
    *
    * @param address The member {@link Address}.
    * @param online  {@code true} if the member has online connection, {@code false} otherwise.
    */
   public void addMember(Address address, boolean online) {
      if (online) {
         onlineOn(address);
      } else {
         offlineOn(address);
      }
   }

   @Override
   protected SiteStatus createMixedStatus(List<Address> onlineElements, List<Address> offlineElements) {
      return new CacheMixedSiteStatus(onlineElements, offlineElements);
   }
}
