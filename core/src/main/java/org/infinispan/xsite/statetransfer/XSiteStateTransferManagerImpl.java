package org.infinispan.xsite.statetransfer;

import static java.lang.String.format;
import static org.infinispan.factories.KnownComponentNames.BLOCKING_EXECUTOR;
import static org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.MaxRetriesPolicy;
import static org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.NO_RETRY;
import static org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.RetryPolicy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.LocalInvocation;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.RetryOnFailureXSiteCommand;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStatusRequestCommand;

/**
 * {@link org.infinispan.xsite.statetransfer.XSiteStateTransferManager} implementation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Listener
@Scope(Scopes.NAMED_CACHE)
public class XSiteStateTransferManagerImpl implements XSiteStateTransferManager {

   private static final Log log = LogFactory.getLog(XSiteStateTransferManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean debug = log.isDebugEnabled();

   @Inject ComponentRegistry componentRegistry;
   @Inject RpcManager rpcManager;
   @Inject Configuration configuration;
   @Inject CommandsFactory commandsFactory;
   @Inject ResponseGenerator responseGenerator;
   // TODO: This should be removed in https://issues.redhat.com/browse/ISPN-11398
   @Inject @ComponentName(BLOCKING_EXECUTOR)
   ExecutorService blockingExecutor;
   @Inject StateTransferManager stateTransferManager;
   @Inject DistributionManager distributionManager;
   @Inject CacheNotifier<?, ?> cacheNotifier;
   @Inject XSiteStateConsumer consumer;
   @Inject XSiteStateProvider provider;

   private final ConcurrentMap<String, XSiteStateTransferCollector> siteCollector;
   private final ConcurrentMap<String, String> status;

   public XSiteStateTransferManagerImpl() {
      siteCollector = new ConcurrentHashMap<>();
      status = new ConcurrentHashMap<>();
   }

   @Start
   public void addListener() {
      cacheNotifier.addListener(this);
   }

   @Stop
   public void removeListener() {
      cacheNotifier.removeListener(this);
   }

   @Override
   public void notifyStatePushFinished(String siteName, Address node, boolean statusOk) throws Throwable {
      XSiteStateTransferCollector collector = siteCollector.get(siteName);
      if (collector == null) {
         return;
      }
      final XSiteBackup xSiteBackup = findSite(siteName);
      if (collector.confirmStateTransfer(node, statusOk)) {
         siteCollector.remove(siteName);
         status.put(siteName, collector.isStatusOk() ? STATUS_OK : STATUS_ERROR);
         CacheRpcCommand command = commandsFactory.buildXSiteStateTransferCancelSendCommand(siteName);
         controlStateTransferOnLocalSite(command); //to force the nodes to cleanup
         XSiteReplicateCommand remoteSiteCommand = commandsFactory.buildXSiteStateTransferFinishReceiveCommand(null);
         controlStateTransferOnRemoteSite(xSiteBackup, remoteSiteCommand, backupRpcConfiguration(siteName));
      }
   }

   @Override
   public final void startPushState(String siteName) throws Throwable {
      //check site name first
      if (siteName == null) {
         throw new NullPointerException("Site name cannot be null!");
      }
      final XSiteBackup xSiteBackup = findSite(siteName);
      if (xSiteBackup == null) {
         throw new IllegalArgumentException("Site " + siteName + " not found!");
      }

      if (siteCollector.putIfAbsent(siteName, new XSiteStateTransferCollector(rpcManager.getMembers())) != null) {
         throw new Exception(format("X-Site state transfer to '%s' already started!", siteName));
      }

      //clear the previous status
      status.remove(siteName);

      try {
         XSiteReplicateCommand remoteSiteCommand = commandsFactory.buildXSiteStateTransferStartReceiveCommand(null);
         controlStateTransferOnRemoteSite(xSiteBackup, remoteSiteCommand, null);
         if (!stateTransferManager.isStateTransferInProgress()) {
            //only if we are in balanced cluster, we start to send the data!
            CacheRpcCommand command = commandsFactory.buildXSiteStateTransferStartSendCommand(siteName, currentTopologyId());
            controlStateTransferOnLocalSite(command);
         } else {
            if (debug) {
               log.debugf("Not start sending keys to site '%s' while rebalance in progress. Wait until it is finished!",
                          siteName);
            }
         }
      } catch (Throwable throwable) {
         handleFailure(xSiteBackup);
         throw new Exception(throwable);
      }
   }

   @Override
   public List<String> getRunningStateTransfers() {
      return siteCollector.isEmpty() ? Collections.emptyList() : new ArrayList<>(siteCollector.keySet());
   }

   @Override
   public Map<String, String> getStatus() {
      return status.isEmpty() ? Collections.emptyMap() : new HashMap<>(status);
   }

   @Override
   public void clearStatus() {
      status.clear();
   }

   @Override
   public void cancelPushState(String siteName) throws Throwable {
      if (!siteCollector.containsKey(siteName)) {
         if (trace) {
            log.tracef("Tried to cancel push state to '%s' but it does not exist.", siteName);
         }
         return;
      }
      final XSiteBackup xSiteBackup = findSite(siteName);
      if (xSiteBackup == null) {
         throw new IllegalArgumentException("Site " + siteName + " not found!");
      }
      CacheRpcCommand command = commandsFactory.buildXSiteStateTransferCancelSendCommand(siteName);
      controlStateTransferOnLocalSite(command);
      XSiteReplicateCommand remoteSiteCommand = commandsFactory.buildXSiteStateTransferFinishReceiveCommand(null);
      controlStateTransferOnRemoteSite(xSiteBackup, remoteSiteCommand, null);
      siteCollector.remove(siteName);
      status.put(siteName, STATUS_CANCELED);
   }

   @Override
   public Map<String, String> getClusterStatus() {
      XSiteStateTransferStatusRequestCommand command = commandsFactory.buildXSiteStateTransferStatusRequestCommand();
      Map<String, String> result = new HashMap<>();

      for (Response response : invokeRemotelyInLocalSite(command).values()) {
         if (response instanceof SuccessfulResponse) {
            //noinspection unchecked
            result.putAll((Map<String, String>) ((SuccessfulResponse<?>) response).getResponseValue());
         }
      }
      return result;
   }

   @Override
   public void clearClusterStatus() throws Exception {
      CacheRpcCommand command = commandsFactory.buildXSiteStateTransferClearStatusCommand();
      controlStateTransferOnLocalSite(command);
   }

   @Override
   public String getSendingSiteName() {
      return consumer.getSendingSiteName();
   }

   @Override
   public void cancelReceive(String siteName) throws Exception {
      CacheRpcCommand command = commandsFactory.buildXSiteStateTransferFinishReceiveCommand(siteName);
      controlStateTransferOnLocalSite(command);
   }

   @Override
   public void becomeCoordinator(String siteName) {
      startCoordinating(Collections.singleton(siteName), rpcManager.getMembers());
      if (stateTransferManager.isStateTransferInProgress()) {
         //cancel all the x-site state transfer until the local site is rebalanced
         try {
            log.debugf("Canceling x-site state transfer for site %s", siteName);
            CacheRpcCommand command = commandsFactory.buildXSiteStateTransferCancelSendCommand(siteName);
            controlStateTransferOnLocalSite(command);
         } catch (Exception e) {
            //not serious... we are going to restart it anyway
            log.debugf(e, "Unable to cancel x-site state transfer for site %s", siteName);
         }
      } else {
         //it is balanced
         log.debugf("Restarting x-site state transfer for site %s", siteName);
         try {
            CacheRpcCommand command = commandsFactory.buildXSiteStateTransferRestartSendingCommand(siteName, currentTopologyId());
            controlStateTransferOnLocalSite(command);
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

   @TopologyChanged
   public <K, V> CompletionStage<Void> handleTopology(TopologyChangedEvent<K, V> topologyChangedEvent) {
      if (debug) {
         log.debugf("Topology change detected! %s", topologyChangedEvent);
      }
      if (topologyChangedEvent.isPre()) {
         return null;
      }
      final List<Address> newMembers = topologyChangedEvent.getWriteConsistentHashAtEnd().getMembers();
      final boolean amINewCoordinator = newMembers.get(0).equals(rpcManager.getAddress());
      Collection<String> missingCoordinatorSites = provider.getSitesMissingCoordinator(new HashSet<>(newMembers));

      if (amINewCoordinator) {
         startCoordinating(missingCoordinatorSites, newMembers);
      }

      // Don't use thread if no sites to notify
      if (siteCollector.isEmpty()) {
         return null;
      }
      if (stateTransferManager.isStateTransferInProgress()) {
         return CompletableFuture.runAsync(() -> {
            //cancel all the x-site state transfer until the local site is rebalanced
            for (String siteName : siteCollector.keySet()) {
               try {
                  log.debugf("Topology change detected! Canceling x-site state transfer for site %s", siteName);
                  CacheRpcCommand command = commandsFactory.buildXSiteStateTransferCancelSendCommand(siteName);
                  controlStateTransferOnLocalSite(command);
               } catch (Exception e) {
                  //not serious... we are going to restart it anyway
                  log.debugf(e, "Unable to cancel x-site state transfer for site %s", siteName);
               }
            }
         }, blockingExecutor);
      } else {
         return CompletableFuture.runAsync(() -> {
            //it is balanced
            for (Map.Entry<String, XSiteStateTransferCollector> entry : siteCollector.entrySet()) {
               entry.setValue(new XSiteStateTransferCollector(newMembers));
               log.debugf("Topology change detected! Restarting x-site state transfer for site %s", entry.getKey());
               try {
                  CacheRpcCommand command = commandsFactory.buildXSiteStateTransferRestartSendingCommand(entry.getKey(), currentTopologyId());
                  controlStateTransferOnLocalSite(command);
               } catch (Exception e) {
                  log.failedToRestartXSiteStateTransfer(entry.getKey(), e);
               }
            }
         }, blockingExecutor);
      }
   }

   private void startCoordinating(Collection<String> sitesName, Collection<Address> members) {
      if (debug) {
         log.debugf("Becoming the x-site state transfer coordinator for %s", sitesName);
      }
      for (String siteName : sitesName) {
         //check site name first
         if (siteName == null) {
            throw new NullPointerException("Site name cannot be null!");
         }
         final XSiteBackup xSiteBackup = findSite(siteName);
         if (xSiteBackup == null) {
            throw new IllegalArgumentException("Site " + siteName + " not found!");
         }

         siteCollector.putIfAbsent(siteName, new XSiteStateTransferCollector(members));
      }
   }

   private void handleFailure(XSiteBackup xSiteBackup) {
      if (debug) {
         log.debugf("Handle start state transfer failure to %s", xSiteBackup.getSiteName());
      }
      siteCollector.remove(xSiteBackup.getSiteName());
      try {
         CacheRpcCommand command = commandsFactory.buildXSiteStateTransferCancelSendCommand(xSiteBackup.getSiteName());
         controlStateTransferOnLocalSite(command);
      } catch (Exception e) {
         if (debug) {
            log.debugf(e, "Exception while cancel sending to remote site %s", xSiteBackup.getSiteName());
         }
      }
      try {
         XSiteReplicateCommand command = commandsFactory.buildXSiteStateTransferFinishReceiveCommand(null);
         controlStateTransferOnRemoteSite(xSiteBackup, command, null);
      } catch (Throwable throwable) {
         if (debug) {
            log.debugf(throwable, "Exception while cancel receiving in remote site %s", xSiteBackup.getSiteName());
         }
      }
   }

   private void controlStateTransferOnRemoteSite(XSiteBackup xSiteBackup, XSiteReplicateCommand command,
                                                 BackupRpcConfiguration backupRpcConfiguration) throws Throwable {
      RetryPolicy retryPolicy = backupRpcConfiguration == null ? NO_RETRY :
            new MaxRetriesPolicy(backupRpcConfiguration.maxRetries);
      long waitTime = backupRpcConfiguration == null ? 1 : backupRpcConfiguration.waitTime;
      RetryOnFailureXSiteCommand remoteSite = RetryOnFailureXSiteCommand.newInstance(xSiteBackup, command, retryPolicy);
      remoteSite.execute(rpcManager, waitTime, TimeUnit.MILLISECONDS);
   }

   private void controlStateTransferOnLocalSite(CacheRpcCommand command) throws Exception {
      for (Map.Entry<Address, Response> entry : invokeRemotelyInLocalSite(command).entrySet()) {
         if (entry.getValue() instanceof ExceptionResponse) {
            throw ((ExceptionResponse) entry.getValue()).getException();
         }
      }
   }

   private int currentTopologyId() {
      return distributionManager.getCacheTopology().getTopologyId();
   }

   private XSiteBackup findSite(String siteName) {
      SitesConfiguration sites = configuration.sites();
      for (BackupConfiguration bc : sites.allBackups()) {
         if (bc.site().equals(siteName)) {
            return new XSiteBackup(bc.site(), true, bc.replicationTimeout());
         }
      }
      return null;
   }

   private BackupRpcConfiguration backupRpcConfiguration(String siteName) {
      SitesConfiguration sites = configuration.sites();
      for (BackupConfiguration bc : sites.allBackups()) {
         if (bc.site().equals(siteName)) {
            return new BackupRpcConfiguration(bc.stateTransfer().waitTime(),
                                              bc.stateTransfer().maxRetries());
         }
      }
      return null;
   }

   private Map<Address, Response> invokeRemotelyInLocalSite(CacheRpcCommand command) {
      CompletionStage<Map<Address, Response>> remoteFuture = rpcManager.invokeCommandOnAll(
            command, MapResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
      Response localResponse;
      try {
         localResponse = LocalInvocation.newInstance(componentRegistry, command).call();
      } catch (Exception e) {
         localResponse = new ExceptionResponse(e);
      }
      final Map<Address, Response> responseMap = rpcManager.blocking(remoteFuture);
      responseMap.put(rpcManager.getAddress(), localResponse);
      return responseMap;
   }

   private static class BackupRpcConfiguration {
      private final long waitTime;
      private final int maxRetries;

      private BackupRpcConfiguration(long waitTime, int maxRetries) {
         this.waitTime = waitTime;
         this.maxRetries = maxRetries;
      }
   }
}
