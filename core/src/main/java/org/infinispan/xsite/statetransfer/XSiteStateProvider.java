package org.infinispan.xsite.statetransfer;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * It contains the logic to send state to another site.
 * <p/>
 * // TODO testing: 1) what happen if the requestor dies? 2) what happen if the cancel arrives first than the start?
 * // TODO test: what happen if the site master dies, topology change, etc... (for second part)
 * // TODO JIRA: ISPN-4025
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface XSiteStateProvider {

   /**
    * It notifies this node to start sending state to the remote site. Also, it should keep information about which node
    * requested the state transfer in order to send back the notification when finishes.
    *
    * @param siteName  the remote site name.
    * @param requestor the requestor.
    */
   public void startStateTransfer(String siteName, Address requestor);

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
}
