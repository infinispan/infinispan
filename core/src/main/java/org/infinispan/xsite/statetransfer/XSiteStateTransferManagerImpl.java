package org.infinispan.xsite.statetransfer;

import static org.infinispan.remoting.transport.impl.VoidResponseCollector.ignoreLeavers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.commands.XSiteAutoTransferStatusCommand;
import org.infinispan.xsite.commands.XSiteStateTransferCancelSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferClearStatusCommand;
import org.infinispan.xsite.commands.XSiteStateTransferFinishReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferRestartSendingCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartSendCommand;
import org.infinispan.xsite.response.AutoStateTransferResponse;
import org.infinispan.xsite.response.AutoStateTransferResponseCollector;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;

/**
 * {@link org.infinispan.xsite.statetransfer.XSiteStateTransferManager} implementation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Scope(Scopes.NAMED_CACHE)
public class XSiteStateTransferManagerImpl implements XSiteStateTransferManager {

   private static final Log log = LogFactory.getLog(XSiteStateTransferManagerImpl.class);
   private static final BiFunction<Throwable, String, Void> DEBUG_CANCEL_FAIL = (t, site) -> {
      log.debugf(t, "Unable to cancel x-site state transfer for site %s", site);
      return null;
   };

   @Inject RpcManager rpcManager;
   @Inject CommandsFactory commandsFactory;
   @Inject XSiteStateConsumer consumer;
   @Inject XSiteStateProvider provider;
   @Inject TakeOfflineManager takeOfflineManager;
   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject String cacheName;

   private final ConcurrentMap<String, RemoteSiteStatus> sites;
   private volatile int currentTopologyId = -1;
   //local cluster state transfer
   private volatile boolean isStateTransferInProgress;

   public XSiteStateTransferManagerImpl(Configuration configuration) {
      sites = new ConcurrentHashMap<>();
      for (BackupConfiguration bc : configuration.sites().allBackups()) {
         sites.put(bc.site(), RemoteSiteStatus.fromConfiguration(bc));
      }
   }

   @Start
   public void start() {
      sites.remove(rpcManager.getTransport().localSiteName());
   }

   @Override
   public void notifyStatePushFinished(String siteName, Address node, boolean statusOk) {
      RemoteSiteStatus status = sites.get(siteName);
      assert status != null; // if this node is coordinating the state transfer, RemoteSiteStatus must exist
      if (status.confirmStateTransfer(node, statusOk)) {
         //state transfer finished. Cleanup local site & remote site
         cancelStateTransferSending(siteName).exceptionally(t -> {
            log.xsiteCancelSendFailed(t, siteName);
            return null;
         });

         if (status.isSync()) {
            //with async cross-site, the remote site doesn't have the concept of state transfer.
            sendStateTransferFinishToRemoteSite(status).exceptionally(t -> {
               log.xsiteCancelReceiveFailed(t, getLocalSite(), siteName);
               return null;
            });
         }
      }
   }

   @Override
   public final void startPushState(String siteName) {
      rpcManager.blocking(asyncStartPushState(validateSite(siteName)));
   }

   @Override
   public List<String> getRunningStateTransfers() {
      return sites.values().stream()
            .filter(RemoteSiteStatus::isStateTransferInProgress)
            .map(RemoteSiteStatus::getSiteName)
            .collect(Collectors.toList());
   }

   @Override
   public Map<String, StateTransferStatus> getStatus() {
      return sites.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getStatus()));
   }

   @Override
   public void clearStatus() {
      sites.values().forEach(RemoteSiteStatus::clearStatus);
   }

   @Override
   public void cancelPushState(String siteName) throws Throwable {
      RemoteSiteStatus status = validateSite(siteName);
      status.cancelStateTransfer();
      AggregateCompletionStage<Void> rsp = CompletionStages.aggregateCompletionStage();

      CompletionStage<Void> cancelSending = cancelStateTransferSending(siteName);
      cancelSending.exceptionally(t -> {
         log.xsiteCancelSendFailed(t, siteName);
         return null;
      });
      rsp.dependsOn(cancelSending);

      if (status.isSync()) {
         //with async cross-site, the remote site doesn't have the concept of state transfer.
         CompletionStage<Void> cancelReceiving = sendStateTransferFinishToRemoteSite(status);
         cancelReceiving.exceptionally(t -> {
            log.xsiteCancelReceiveFailed(t, getLocalSite(), siteName);
            return null;
         });
         rsp.dependsOn(cancelReceiving);
      }
      rsp.freeze().toCompletableFuture().join();
   }

   @Override
   public Map<String, StateTransferStatus> getClusterStatus() {
      CacheRpcCommand command = commandsFactory.buildXSiteStateTransferStatusRequestCommand();
      StatusResponseCollector collector = new StatusResponseCollector();
      getStatus().forEach(collector); //add local status
      return rpcManager.blocking(rpcManager.invokeCommandOnAll(command, collector, rpcManager.getSyncRpcOptions()));
   }

   @Override
   public void clearClusterStatus() {
      XSiteStateTransferClearStatusCommand cmd = commandsFactory.buildXSiteStateTransferClearStatusCommand();
      CompletionStage<Void> rsp = rpcManager.invokeCommandOnAll(cmd, ignoreLeavers(), rpcManager.getSyncRpcOptions());
      cmd.invokeLocal(this);
      rpcManager.blocking(rsp);
   }

   @Override
   public String getSendingSiteName() {
      return consumer.getSendingSiteName();
   }

   @Override
   public void cancelReceive(String siteName) {
      XSiteStateTransferFinishReceiveCommand cmd = commandsFactory.buildXSiteStateTransferFinishReceiveCommand(siteName);
      CompletionStage<Void> rsp = sendToLocalSite(cmd);
      cmd.invokeLocal(consumer);
      rpcManager.blocking(rsp);
   }

   @Override
   public void becomeCoordinator(String siteName) {
      startCoordinating(Collections.singleton(siteName), rpcManager.getMembers());
      if (isStateTransferInProgress) {
         //cancel all the x-site state transfer until the local site is rebalanced
         doCancelSendingForRestart(siteName);
      } else {
         //it is balanced
         if (log.isDebugEnabled()) {
            log.debugf("Restarting x-site state transfer for site %s", siteName);
         }
         try {
            rpcManager.blocking(restartStateTransferSending(siteName));
         } catch (Exception e) {
            log.failedToRestartXSiteStateTransfer(siteName, e);
         }
      }
   }

   @Override
   public XSiteStateProvider getStateProvider() {
      return provider;
   }

   @Override
   public XSiteStateConsumer getStateConsumer() {
      return consumer;
   }

   @Override
   public void startAutomaticStateTransfer(Collection<String> sites) {
      for (String site : sites) {
         doAutomaticStateTransfer(site);
      }
   }

   @Override
   public XSiteStateTransferMode stateTransferMode(String site) {
      RemoteSiteStatus status = sites.get(site);
      return status == null ? XSiteStateTransferMode.MANUAL : status.stateTransferMode();
   }

   @Override
   public boolean setAutomaticStateTransfer(String site, XSiteStateTransferMode mode) {
      RemoteSiteStatus status = sites.get(site);
      return status != null && status.setStateTransferMode(mode);
   }

   private void doAutomaticStateTransfer(String site) {
      RemoteSiteStatus status = sites.get(site);
      if (skipAutomaticStateTransferEnabled(site, status)) {
         return;
      }
      isStateTransferRequired(status).whenComplete((proceed, throwable) -> {
         if (throwable != null) {
            Log.XSITE.unableToStartXSiteAutStateTransfer(cacheName, site, throwable);
         } else if (proceed) {
            bringSiteOnline(site).thenRun(() -> asyncStartPushState(status));
         } else {
            Log.XSITE.debugf("[%s] Cross-Site state transfer not required for site '%s'", cacheName, site);
         }
      });
   }

   private boolean skipAutomaticStateTransferEnabled(String site, RemoteSiteStatus status) {
      if (status == null) {
         Log.XSITE.debugf("[%s] Cross-Site automatic state transfer not started for site '%s'. It is not a backup location for this cache", cacheName, site);
         return true;
      }
      if (status.isSync()) {
         Log.XSITE.debugf("[%s] Cross-Site automatic state transfer not started for site '%s'. The backup strategy is set to SYNC", cacheName, site);
         return true;
      }
      if (status.stateTransferMode() == XSiteStateTransferMode.MANUAL) {
         Log.XSITE.debugf("[%s] Cross-Site automatic state transfer not started for site '%s'. Automatic state transfer is disabled", cacheName, site);
         return true;
      }
      return false;
   }

   private CompletionStage<Boolean> isStateTransferRequired(RemoteSiteStatus status) {
      final String site = status.getSiteName();
      AutoStateTransferResponseCollector collector = new AutoStateTransferResponseCollector(takeOfflineManager.getSiteState(site) == SiteState.OFFLINE, status.stateTransferMode());
      XSiteAutoTransferStatusCommand cmd = commandsFactory.buildXSiteAutoTransferStatusCommand(site);
      return rpcManager.invokeCommandOnAll(cmd, collector, rpcManager.getSyncRpcOptions())
            .thenApply(AutoStateTransferResponse::canDoAutomaticStateTransfer);
   }

   private CompletionStage<Void> bringSiteOnline(String site) {
      CacheRpcCommand cmd = commandsFactory.buildXSiteBringOnlineCommand(site);
      CompletionStage<Void> rsp = rpcManager.invokeCommandOnAll(cmd, VoidResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
      takeOfflineManager.bringSiteOnline(site);
      return rsp;
   }

   @Override
   public void onTopologyUpdated(CacheTopology cacheTopology, boolean stateTransferInProgress) {
      int topologyId = cacheTopology.getTopologyId();
      if (log.isDebugEnabled()) {
         log.debugf("Topology change. TopologyId: %s. State transfer in progress? %s", Integer.toString(topologyId), stateTransferInProgress);
      }
      this.currentTopologyId = topologyId;
      this.isStateTransferInProgress = stateTransferInProgress;
      final List<Address> newMembers = cacheTopology.getMembers();
      final boolean amINewCoordinator = newMembers.get(0).equals(rpcManager.getAddress());
      Collection<String> missingCoordinatorSites = provider.getSitesMissingCoordinator(new HashSet<>(newMembers));
      if (amINewCoordinator) {
         startCoordinating(missingCoordinatorSites, newMembers);
      }

      if (stateTransferInProgress) {
         //cancel all the x-site state transfer until the local site is rebalanced
         sites.values().stream()
               .filter(RemoteSiteStatus::isStateTransferInProgress)
               .map(RemoteSiteStatus::getSiteName)
               .forEach(this::doCancelSendingForRestart);
      } else {
         //it is balanced
         for (RemoteSiteStatus status : sites.values()) {
            if (!status.restartStateTransfer(newMembers)) {
               continue;
            }
            String siteName = status.getSiteName();
            if (log.isDebugEnabled()) {
               log.debugf("Topology change detected! Restarting x-site state transfer for site %s", siteName);
            }
            try {
               restartStateTransferSending(siteName);
            } catch (Exception e) {
               log.failedToRestartXSiteStateTransfer(siteName, e);
            }
         }
      }
   }

   private CompletionStage<Void> asyncStartPushState(RemoteSiteStatus status) {
      String siteName = status.getSiteName();
      if (!status.startStateTransfer(rpcManager.getMembers())) {
         CompletableFutures.completedExceptionFuture(log.xsiteStateTransferAlreadyInProgress(siteName));
      }

      CompletionStage<Void> rsp = null;
      if (status.isSync()) {
         //with async cross-site, the remote site doesn't have the concept of state transfer.
         //the remote site receives normal updates and apply conflict resolution if required.
         XSiteReplicateCommand<Void> remoteSiteCommand = commandsFactory.buildXSiteStateTransferStartReceiveCommand();
         rsp = rpcManager.invokeXSite(status.getBackup(), remoteSiteCommand);
      }

      if (isStateTransferInProgress) {
         if (log.isDebugEnabled()) {
            log.debugf("Not starting state transfer to site '%s' while rebalance in progress. Waiting until it is finished!",
                  siteName);
         }
         return rsp == null ? CompletableFutures.completedNull() : rsp;
      }


      if (rsp == null) {
         return asyncStartLocalSend(status);
      } else {
         return rsp.thenCompose(o -> asyncStartLocalSend(status));
      }
   }

   private CompletionStage<Void> asyncStartLocalSend(RemoteSiteStatus status) {
      XSiteStateTransferStartSendCommand cmd = commandsFactory.buildXSiteStateTransferStartSendCommand(status.getSiteName(), currentTopologyId);
      CompletionStage<Void> rsp = sendToLocalSite(cmd);
      cmd.setOrigin(rpcManager.getAddress());
      cmd.invokeLocal(provider);
      rsp.exceptionally(throwable -> {
         handleFailure(status, throwable);
         return null;
      });
      return rsp;
   }

   private String getLocalSite() {
      return rpcManager.getTransport().localSiteName();
   }

   private void doCancelSendingForRestart(String siteName) {
      try {
         if (log.isDebugEnabled()) {
            log.debugf("Canceling x-site state transfer for site %s", siteName);
         }
         CompletionStage<Void> rsp = cancelStateTransferSending(siteName);
         if (log.isDebugEnabled()) {
            rsp.exceptionally(t -> DEBUG_CANCEL_FAIL.apply(t, siteName));
         }
      } catch (Exception e) {
         //not serious... we are going to restart it anyway
         if (log.isDebugEnabled()) {
            DEBUG_CANCEL_FAIL.apply(e, siteName);
         }
      }
   }

   private RemoteSiteStatus validateSite(String siteName) throws NullPointerException, IllegalArgumentException {
      RemoteSiteStatus status = sites.get(Objects.requireNonNull(siteName, "Site name cannot be null."));
      if (status == null) {
         throw log.siteNotFound(siteName);
      }
      return status;
   }

   private CompletionStage<Void> cancelStateTransferSending(String siteName) {
      XSiteStateTransferCancelSendCommand cmd = commandsFactory.buildXSiteStateTransferCancelSendCommand(siteName);
      CompletionStage<Void> rsp = sendToLocalSite(cmd);
      cmd.invokeLocal(provider);
      return rsp;
   }

   private CompletionStage<Void> restartStateTransferSending(String siteName) {
      int topologyId = currentTopologyId;
      XSiteStateTransferRestartSendingCommand cmd = commandsFactory.buildXSiteStateTransferRestartSendingCommand(siteName, topologyId);
      CompletionStage<Void> rsp = sendToLocalSite(cmd);
      cmd.setOrigin(rpcManager.getAddress());
      cmd.invokeLocal(provider);
      return rsp;
   }

   private void startCoordinating(Collection<String> sitesName, Collection<Address> members) {
      if (log.isDebugEnabled()) {
         log.debugf("Becoming the x-site state transfer coordinator for %s", sitesName);
      }
      for (String siteName : sitesName) {
         RemoteSiteStatus status = sites.get(siteName);
         assert status != null;
         status.startStateTransfer(members);
      }
   }

   private void handleFailure(RemoteSiteStatus siteStatus, Throwable throwable) {
      final String siteName = siteStatus.getSiteName();
      if (log.isDebugEnabled()) {
         log.debugf(throwable, "Handle start state transfer failure to %s", siteName);
      }
      siteStatus.failStateTransfer();
      CompletionStage<Void> rsp = cancelStateTransferSending(siteName);
      if (log.isDebugEnabled()) {
         rsp.exceptionally(t -> DEBUG_CANCEL_FAIL.apply(t, siteName));
      }

      if (!siteStatus.isSync()) {
         return;
      }

      //with async cross-site, the remote site doesn't have the concept of state transfer.
      rsp = sendStateTransferFinishToRemoteSite(siteStatus);
      if (log.isDebugEnabled()) {
         rsp.exceptionally(t -> {
            log.debugf(t, "Exception while cancel receiving in remote site %s", siteName);
            return null;
         });

      }
   }

   private CompletionStage<Void> sendToLocalSite(CacheRpcCommand command) {
      return rpcManager.invokeCommandOnAll(command, ignoreLeavers(), fifoSyncRpcOptions());
   }

   private CompletionStage<Void> sendStateTransferFinishToRemoteSite(RemoteSiteStatus status) {
      return rpcManager.invokeXSite(status.getBackup(), commandsFactory.buildXSiteStateTransferFinishReceiveCommand(null));
   }

   private RpcOptions fifoSyncRpcOptions() {
      RpcOptions sync = rpcManager.getSyncRpcOptions();
      return new RpcOptions(DeliverOrder.PER_SENDER, sync.timeout(), sync.timeUnit());
   }
}
