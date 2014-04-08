package org.infinispan.xsite.statetransfer;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * It contains the logic to send state to another site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface XSiteStateProvider {

   /**
    * It notifies this node to start sending state to the remote site. Also, it should keep information about which node
    * requested the state transfer in order to send back the notification when finishes.
    *  @param siteName  the remote site name.
    * @param requestor the requestor.
    * @param minTopologyId
    */
   public void startStateTransfer(String siteName, Address requestor, int minTopologyId);

   /**
    * It cancels the state transfer for the remote site. If no state transfer is available, it should do nothing.
    *
    * @param siteName the remote site name.
    */
   public void cancelStateTransfer(String siteName);

   /**
    * @return a site name collection with the sites in which this cache is sending state.
    */
   public Collection<String> getCurrentStateSending();

   /**
    * @return a site name collection with sites in which the coordinator is not in the {@code currentMembers}.
    */
   public Collection<String> getSitesMissingCoordinator(Collection<Address> currentMembers);
}
