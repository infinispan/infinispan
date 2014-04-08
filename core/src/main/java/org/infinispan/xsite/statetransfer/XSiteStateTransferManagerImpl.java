package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
import org.infinispan.commons.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
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
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.*;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.StateTransferControl;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.StateTransferControl.*;

/**
 * {@link org.infinispan.xsite.statetransfer.XSiteStateTransferManager} implementation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Listener
public class XSiteStateTransferManagerImpl implements XSiteStateTransferManager {

   private static final Log log = LogFactory.getLog(XSiteStateTransferManagerImpl.class);
   private static final boolean debug = log.isDebugEnabled();
   private final ConcurrentMap<String, XSiteStateTransferCollector> siteCollector;
   private final ConcurrentMap<String, String> status;
   private RpcManager rpcManager;
   private Configuration configuration;
   private CommandsFactory commandsFactory;
   private ResponseGenerator responseGenerator;
   private ExecutorService asyncExecutor;
   private StateTransferManager stateTransferManager;
   private CacheNotifier cacheNotifier;
   private XSiteStateConsumer consumer;
   private XSiteStateProvider provider;

   public XSiteStateTransferManagerImpl() {
      siteCollector = CollectionFactory.makeConcurrentMap();
      status = CollectionFactory.makeConcurrentMap();
   }

   @Inject
   public void inject(RpcManager rpcManager, Configuration configuration, CommandsFactory commandsFactory,
                      ResponseGenerator responseGenerator, StateTransferManager stateTransferManager,
                      CacheNotifier cacheNotifier, XSiteStateConsumer consumer, XSiteStateProvider provider,
                      @ComponentName(value = ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncExecutor) {
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.commandsFactory = commandsFactory;
      this.responseGenerator = responseGenerator;
      this.asyncExecutor = asyncExecutor;
      this.stateTransferManager = stateTransferManager;
      this.cacheNotifier = cacheNotifier;
      this.consumer = consumer;
      this.provider = provider;
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
         controlStateTransferOnLocalSite(StateTransferControl.CANCEL_SEND, siteName); //to force the nodes to cleanup
         controlStateTransferOnRemoteSite(xSiteBackup, StateTransferControl.FINISH_RECEIVE, backupRpcConfiguration(siteName));
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

      try {
         controlStateTransferOnRemoteSite(xSiteBackup, StateTransferControl.START_RECEIVE, null);
         if (!stateTransferManager.isStateTransferInProgress()) {
            //only if we are in balanced cluster, we start to send the data!
            controlStateTransferOnLocalSite(StateTransferControl.START_SEND, siteName);
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
      return siteCollector.isEmpty() ? Collections.<String>emptyList() : new ArrayList<>(siteCollector.keySet());
   }

   @Override
   public Map<String, String> getStatus() {
      return status.isEmpty() ? Collections.<String, String>emptyMap() : new HashMap<>(status);
   }

   @Override
   public void clearStatus() {
      status.clear();
   }

   @Override
   public void cancelPushState(String siteName) throws Throwable {
      final XSiteBackup xSiteBackup = findSite(siteName);
      if (xSiteBackup == null) {
         throw new IllegalArgumentException("Site " + siteName + " not found!");
      }
      controlStateTransferOnLocalSite(CANCEL_SEND, siteName);
      controlStateTransferOnRemoteSite(xSiteBackup, FINISH_RECEIVE, null);

   }

   @Override
   public Map<String, String> getClusterStatus() throws Exception {
      CacheRpcCommand command = commandsFactory.buildXSiteStateTransferControlCommand(STATUS_REQUEST, null);
      Map<String, String> result = new HashMap<>();

      for (Response response : invokeRemotelyInLocalSite(command).values()) {
         if (response instanceof SuccessfulResponse) {
            //noinspection unchecked
            result.putAll((Map<String, String>) ((SuccessfulResponse) response).getResponseValue());
         }
      }
      return result;
   }

   @Override
   public void clearClusterStatus() throws Exception {
      controlStateTransferOnLocalSite(CLEAR_STATUS, null);
   }

   @Override
   public String getSendingSiteName() {
      return consumer.getSendingSiteName();
   }

   @Override
   public void cancelReceive(String siteName) throws Exception {
      controlStateTransferOnLocalSite(FINISH_RECEIVE, siteName);
   }

   @Override
   public void becomeCoordinator(String siteName) {
      startCoordinating(Collections.singleton(siteName), rpcManager.getMembers());
      if (stateTransferManager.isStateTransferInProgress()) {
         //cancel all the x-site state transfer until the local site is rebalanced
         try {
            log.debugf("Canceling x-site state transfer for site %s", siteName);
            controlStateTransferOnLocalSite(StateTransferControl.CANCEL_SEND, siteName);
         } catch (Exception e) {
            //not serious... we are going to restart it anyway
            log.debugf(e, "Unable to cancel x-site state transfer for site %s", siteName);
         }
      } else {
         //it is balanced
         log.debugf("Restarting x-site state transfer for site %s", siteName);
         try {
            controlStateTransferOnLocalSite(StateTransferControl.RESTART_SEND, siteName);
         } catch (Exception e) {
            log.failedToRestartXSiteStateTransfer(siteName, e);
         }

      }
   }

   @TopologyChanged
   public <K, V> void handleTopology(TopologyChangedEvent<K, V> topologyChangedEvent) {
      if (debug) {
         log.debugf("Topology change detected! %s", topologyChangedEvent);
      }
      if (topologyChangedEvent.isPre()) {
         return;
      }
      final List<Address> newMembers = topologyChangedEvent.getConsistentHashAtEnd().getMembers();
      final boolean amINewCoordinator = newMembers.get(0).equals(rpcManager.getAddress());
      Collection<String> missingCoordinatorSites = provider.getSitesMissingCoordinator(new HashSet<>(newMembers));

      if (amINewCoordinator) {
         startCoordinating(missingCoordinatorSites, newMembers);
      }

      if (stateTransferManager.isStateTransferInProgress()) {
         //cancel all the x-site state transfer until the local site is rebalanced
         for (String siteName : siteCollector.keySet()) {
            try {
               log.debugf("Topology change detected! Canceling x-site state transfer for site %s", siteName);
               controlStateTransferOnLocalSite(StateTransferControl.CANCEL_SEND, siteName);
            } catch (Exception e) {
               //not serious... we are going to restart it anyway
               log.debugf(e, "Unable to cancel x-site state transfer for site %s", siteName);
            }
         }
      } else {
         //it is balanced
         for (Map.Entry<String, XSiteStateTransferCollector> entry : siteCollector.entrySet()) {
            entry.setValue(new XSiteStateTransferCollector(newMembers));
            log.debugf("Topology change detected! Restarting x-site state transfer for site %s", entry.getKey());
            try {
               controlStateTransferOnLocalSite(StateTransferControl.RESTART_SEND, entry.getKey());
            } catch (Exception e) {
               log.failedToRestartXSiteStateTransfer(entry.getKey(), e);
            }
         }

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
         controlStateTransferOnLocalSite(StateTransferControl.CANCEL_SEND, xSiteBackup.getSiteName());
      } catch (Exception e) {
         if (debug) {
            log.debugf(e, "Exception while cancel sending to remote site %s", xSiteBackup.getSiteName());
         }
      }
      try {
         controlStateTransferOnRemoteSite(xSiteBackup, StateTransferControl.FINISH_RECEIVE, null);
      } catch (Throwable throwable) {
         if (debug) {
            log.debugf(throwable, "Exception while cancel receiving in remote site %s", xSiteBackup.getSiteName());
         }
      }
   }

   private void controlStateTransferOnRemoteSite(XSiteBackup xSiteBackup, StateTransferControl control,
                                                 BackupRpcConfiguration backupRpcConfiguration) throws Throwable {
      XSiteStateTransferControlCommand command = commandsFactory.buildXSiteStateTransferControlCommand(control, null);
      RetryPolicy retryPolicy = backupRpcConfiguration == null ? NO_RETRY :
            new MaxRetriesPolicy(backupRpcConfiguration.maxRetries);
      long waitTime = backupRpcConfiguration == null ? 1 : backupRpcConfiguration.waitTime;
      RetryOnFailureXSiteCommand remoteSite = RetryOnFailureXSiteCommand.newInstance(xSiteBackup, command, retryPolicy);
      remoteSite.execute(rpcManager.getTransport(), waitTime, TimeUnit.MILLISECONDS);
   }

   private void controlStateTransferOnLocalSite(StateTransferControl control, String siteName) throws Exception {
      XSiteStateTransferControlCommand command = commandsFactory.buildXSiteStateTransferControlCommand(control, siteName);
      command.setTopologyId(currentTopologyId());
      for (Map.Entry<Address, Response> entry : invokeRemotelyInLocalSite(command).entrySet()) {
         if (entry.getValue() instanceof ExceptionResponse) {
            throw ((ExceptionResponse) entry.getValue()).getException();
         }
      }
   }

   private int currentTopologyId() {
      return stateTransferManager.getCacheTopology().getTopologyId();
   }

   private XSiteBackup findSite(String siteName) {
      SitesConfiguration sites = configuration.sites();
      for (BackupConfiguration bc : sites.allBackups()) {
         if (bc.site().equals(siteName)) {
            return new XSiteBackup(bc.site(), true, bc.stateTransfer().timeout());
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

   private Map<Address, Response> invokeRemotelyInLocalSite(CacheRpcCommand command) throws Exception {
      commandsFactory.initializeReplicableCommand(command, false);
      final NotifyingNotifiableFuture<Map<Address, Response>> remoteFuture = new NotifyingFutureImpl<>();
      rpcManager.invokeRemotelyInFuture(remoteFuture, null, command, rpcManager.getDefaultRpcOptions(true, false));
      final Future<Response> localFuture = asyncExecutor.submit(
            LocalInvocation.newInstance(responseGenerator, command, commandsFactory, rpcManager.getAddress()));
      final Map<Address, Response> responseMap = new HashMap<>();
      responseMap.put(rpcManager.getAddress(), localFuture.get());
      //noinspection unchecked
      responseMap.putAll(remoteFuture.get());
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
