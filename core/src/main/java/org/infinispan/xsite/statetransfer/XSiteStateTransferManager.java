package org.infinispan.xsite.statetransfer;

import org.infinispan.remoting.transport.Address;

import java.util.List;
import java.util.Map;

/**
 * It manages the state transfer between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface XSiteStateTransferManager {

   public static final String STATUS_OK = "OK";
   public static final String STATUS_ERROR = "ERROR";

   /**
    * It receives the notifications from local site when some node finishes pushing the state to the remote site.
    *
    * @param siteName the remote site name
    * @param node     the {@link org.infinispan.remoting.transport.Address} from the node that finishes.
    * @param statusOk {@code true} if no error or exception occurred during the state transfer.
    * @throws Throwable If some unexpected behavior occurs.
    */
   public void notifyStatePushFinished(String siteName, Address node, boolean statusOk) throws Throwable;

   /**
    * It notifies all nodes from local site to start transfer the state to the remote site.
    *
    * @param siteName the remote site name
    * @throws Throwable If some unexpected behavior occurs.
    */
   public void startPushState(String siteName) throws Throwable;

   /**
    * It cancels a running state transfer.
    *
    * @param siteName the site name to where the state is being sent.
    * @throws Throwable if some exception occurs during the remote invocation with the local cluster or remote site.
    */
   void cancelPushState(String siteName) throws Throwable;

   /**
    * @return a list of site names in which this cache is pushing state.
    */
   public List<String> getRunningStateTransfers();

   /**
    * @return the completed state transfer status for which this node is the coordinator.
    */
   public Map<String, String> getStatus();

   /**
    * Clears the completed state transfer status.
    */
   public void clearStatus();

   /**
    * @return the completed state transfer status from all the coordinators in the cluster.
    * @throws Exception if some exception during the remote invocation occurs.
    */
   Map<String, String> getClusterStatus() throws Exception;

   /**
    * Clears the completed state transfer status in all the cluster.
    *
    * @throws Exception if some exception occurs during the remote invocation.
    */
   void clearClusterStatus() throws Exception;

   /**
    * @return {@code null} if this node is not receiving state or the site name which is sending the state.
    */
   String getSendingSiteName();

   /**
    * Sets the cluster to normal state.
    * <p/>
    * The main use for this method is when the link between the sites is broken and the receiver site keeps it state
    * transfer state forever.
    *
    * @param siteName the site name which is sending the state.
    * @throws Exception if some exception occurs during the remote invocation.
    */
   void cancelReceive(String siteName) throws Exception;

   /**
    * Makes this node the coordinator for the state transfer to the site name.
    * <p/>
    * This method is invoked when the coordinator dies and this node receives a late start state transfer request.
    *
    * @param siteName the site name.
    */
   public void becomeCoordinator(String siteName);
}
