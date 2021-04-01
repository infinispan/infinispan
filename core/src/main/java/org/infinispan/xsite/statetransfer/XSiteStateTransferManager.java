package org.infinispan.xsite.statetransfer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

/**
 * It manages the state transfer between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface XSiteStateTransferManager {

   String STATUS_OK = "OK";
   String STATUS_ERROR = "ERROR";
   String STATUS_SENDING = "SENDING";
   String STATUS_CANCELED = "CANCELED";

   /**
    * It receives the notifications from local site when some node finishes pushing the state to the remote site.
    *
    * @param siteName the remote site name
    * @param node     the {@link Address} from the node that finishes.
    * @param statusOk {@code true} if no error or exception occurred during the state transfer.
    */
   void notifyStatePushFinished(String siteName, Address node, boolean statusOk);

   /**
    * It notifies all nodes from local site to start transfer the state to the remote site.
    *
    * @param siteName the remote site name
    * @throws Throwable If some unexpected behavior occurs.
    */
   void startPushState(String siteName) throws Throwable;

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
   List<String> getRunningStateTransfers();

   /**
    * @return the completed state transfer status for which this node is the coordinator.
    */
   Map<String, StateTransferStatus> getStatus();

   /**
    * Clears the completed state transfer status.
    */
   void clearStatus();

   /**
    * @return the completed state transfer status from all the coordinators in the cluster.
    */
   Map<String, StateTransferStatus> getClusterStatus();

   /**
    * Clears the completed state transfer status in all the cluster.
    */
   void clearClusterStatus();

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
   void becomeCoordinator(String siteName);

   /**
    * Notifies {@link XSiteStateTransferManager} that a new {@link CacheTopology} is installed and if the local cluster
    * state transfer is in progress (or about to start)
    *
    * @param cacheTopology           The new {@link CacheTopology}.
    * @param stateTransferInProgress {@code true} if the state transfer is in progress or starting.
    */
   void onTopologyUpdated(CacheTopology cacheTopology, boolean stateTransferInProgress);

   /**
    * @return The {@link XSiteStateProvider} instance.
    */
   XSiteStateProvider getStateProvider();

   /**
    * @return The {@link XSiteStateConsumer} instance.
    */
   XSiteStateConsumer getStateConsumer();

   /**
    * Starts cross-site state transfer for the remote sites, if required.
    *
    * @param sites The remote sites.
    */
   void startAutomaticStateTransfer(Collection<String> sites);

   /**
    * @param site The remote site.
    * @return The {@link XSiteStateTransferMode} configured for the remote site.
    */
   XSiteStateTransferMode stateTransferMode(String site);

   /**
    * Sets the {@link XSiteStateTransferMode} to the remote site.
    * <p>
    * If the configuration for the remote site does not support the {@link XSiteStateTransferMode}, then this method returns
    * {@code false}.
    *
    * @param site The remote site.
    * @param mode The new {@link XSiteStateTransferMode}.
    * @return {@code false} if the site does not support the corresponding {@link XSiteStateTransferMode}.
    */
   boolean setAutomaticStateTransfer(String site, XSiteStateTransferMode mode);
}
