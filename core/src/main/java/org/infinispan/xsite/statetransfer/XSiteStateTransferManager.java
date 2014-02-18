package org.infinispan.xsite.statetransfer;

import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * It manages the state transfer between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface XSiteStateTransferManager {

   /**
    * It receives the notifications from local site when some node finishes pushing the state to the remote site.
    *
    * @param siteName the remote site name
    * @param node     the {@link org.infinispan.remoting.transport.Address} from the node that finishes.
    * @throws Throwable If some unexpected behavior occurs.
    */
   public void notifyStatePushFinished(String siteName, Address node) throws Throwable;

   /**
    * It notifies all nodes from local site to start transfer the state to the remote site.
    *
    * @param siteName the remote site name
    * @throws Throwable If some unexpected behavior occurs.
    */
   public void startPushState(String siteName) throws Throwable;

   /**
    * @return a list of site names in which this cache is pushing state.
    */
   public List<String> getRunningStateTransfers();

}
