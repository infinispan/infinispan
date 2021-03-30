package org.infinispan.xsite.statetransfer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

/**
 * A no-op implementation of {@link XSiteStateTransferManager}.
 *
 * This instance is used when cross-site replication is disabled.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Scope(value = Scopes.NAMED_CACHE)
public class NoOpXSiteStateTransferManager implements XSiteStateTransferManager {

   @Inject XSiteStateConsumer consumer;

   public NoOpXSiteStateTransferManager() {
   }

   @Override
   public void notifyStatePushFinished(String siteName, Address node, boolean statusOk) {
      // no-op
   }

   @Override
   public void startPushState(String siteName) {
      // no-op
   }

   @Override
   public void cancelPushState(String siteName) {
      // no-op
   }

   @Override
   public List<String> getRunningStateTransfers() {
      return Collections.emptyList();
   }

   @Override
   public Map<String, StateTransferStatus> getStatus() {
      return Collections.emptyMap();
   }

   @Override
   public void clearStatus() {
      // no-op
   }

   @Override
   public Map<String, StateTransferStatus> getClusterStatus() {
      return Collections.emptyMap();
   }

   @Override
   public void clearClusterStatus() {
      // no-op
   }

   @Override
   public String getSendingSiteName() {
      return null;
   }

   @Override
   public void cancelReceive(String siteName) {
      // no-op
   }

   @Override
   public void becomeCoordinator(String siteName) {
      // no-op
   }

   @Override
   public void onTopologyUpdated(CacheTopology cacheTopology, boolean stateTransferInProgress) {
      //no-op
   }

   @Override
   public XSiteStateProvider getStateProvider() {
      return NoOpXSiteStateProvider.getInstance();
   }

   @Override
   public XSiteStateConsumer getStateConsumer() {
      //although xsite is disabled, this class is still able to receive state from a remote site.
      return consumer;
   }

   @Override
   public void startAutomaticStateTransfer(Collection<String> sites) {
      //no-op
   }

   @Override
   public XSiteStateTransferMode stateTransferMode(String site) {
      return XSiteStateTransferMode.MANUAL;
   }

   @Override
   public boolean setAutomaticStateTransfer(String site, XSiteStateTransferMode mode) {
      return false;
   }

   @Override
   public String toString() {
      return "NoOpXSiteStateTransferManager{}";
   }
}
