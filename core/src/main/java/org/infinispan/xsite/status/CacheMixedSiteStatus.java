package org.infinispan.xsite.status;

import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * A mixed {@link SiteStatus}.
 * <p>
 * Used per cache and it describes the nodes in which the site is online and offline.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class CacheMixedSiteStatus extends AbstractMixedSiteStatus<Address> {
   public CacheMixedSiteStatus(List<Address> onlineMembers, List<Address> offlineMembers) {
      super(onlineMembers, offlineMembers);
   }
}
