package org.infinispan.xsite.statetransfer;

import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.commands.XSiteStateTransferFinishSendCommand;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Predicate;

/**
 * It contains the logic to send state to another site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Scope(Scopes.NAMED_CACHE)
public class XSiteStateProviderImpl implements XSiteStateProvider {

   private static final Log log = LogFactory.getLog(XSiteStateProviderImpl.class);
   private static final Predicate<InternalCacheEntry<Object, Object>> NOT_L1_ENTRY = e -> !e.isL1Entry();

   @Inject InternalDataContainer<Object, Object> dataContainer;
   @Inject PersistenceManager persistenceManager;
   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject CommandsFactory commandsFactory;
   @Inject RpcManager rpcManager;
   @Inject ComponentRef<XSiteStateTransferManager> stateTransferManager;
   @Inject StateTransferLock stateTransferLock;
   @Inject
   @ComponentName(NON_BLOCKING_EXECUTOR)
   ExecutorService nonBlockingExecutor;
   @Inject
   @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService timeoutExecutor;

   private final ConcurrentMap<String, XSiteStateProviderState> sites;

   public XSiteStateProviderImpl(Configuration configuration) {
      sites = new ConcurrentHashMap<>();
      for (BackupConfiguration backupConfiguration : configuration.sites().allBackups()) {
         sites.put(backupConfiguration.site(), XSiteStateProviderState.fromBackupConfiguration(backupConfiguration));
      }
   }

   @Start
   public void start() {
      sites.remove(rpcManager.getTransport().localSiteName());
   }

   @Override
   public void startStateTransfer(String siteName, Address origin, int minTopologyId) {
      XSiteStateProviderState state = sites.get(siteName);
      assert state != null; //invoked from XSiteStateTransferManager, so the site name must exist

      XSiteStatePushTask task = state.createPushTask(origin, this);

      if (task == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Do not start state transfer to site '%s'. It has already started!", siteName);
         }
         checkCoordinatorAlive(siteName, origin);
         return;
      }

      if (log.isDebugEnabled()) {
         log.debugf("Starting state transfer to site '%s'", siteName);
      }

      IntSet segments = localPrimarySegments();
      Flowable<XSiteState> flowable = Flowable.concat(publishDataContainerEntries(segments), publishStoreEntries(segments));
      task.execute(flowable, stateTransferLock.topologyFuture(minTopologyId));

      checkCoordinatorAlive(siteName, origin);
   }

   @Override
   public void cancelStateTransfer(String siteName) {
      XSiteStateProviderState state = sites.get(siteName);
      assert state != null; //invoked from XSiteStateTransferManager, so the site name must exist
      state.cancelTransfer();
   }

   @Override
   public Collection<String> getCurrentStateSending() {
      return sites.entrySet().stream()
            .filter(e -> e.getValue().isSending())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
   }

   @Override
   public Collection<String> getSitesMissingCoordinator(Collection<Address> currentMembers) {
      return sites.entrySet().stream()
            .filter(e -> e.getValue().isOriginatorMissing(currentMembers))
            .map(Map.Entry::getKey).collect(Collectors.toList());
   }

   @Override
   public void notifyStateTransferEnd(final String siteName, final Address origin, final boolean statusOk) {
      if (log.isDebugEnabled()) {
         log.debugf("Finished state transfer to site '%s'. Ok? %s", siteName, statusOk);
      }
      if (rpcManager.getAddress().equals(origin)) {
         stateTransferManager.running().notifyStatePushFinished(siteName, origin, statusOk);
      } else {
         XSiteStateTransferFinishSendCommand command = commandsFactory.buildXSiteStateTransferFinishSendCommand(siteName, statusOk);
         rpcManager.sendTo(origin, command, DeliverOrder.NONE);
      }
   }

   @Override
   public CommandsFactory getCommandsFactory() {
      return commandsFactory;
   }

   @Override
   public RpcManager getRpcManager() {
      return rpcManager;
   }

   @Override
   public ScheduledExecutorService getScheduledExecutorService() {
      return timeoutExecutor;
   }

   @Override
   public Executor getExecutor() {
      return nonBlockingExecutor;
   }

   private void checkCoordinatorAlive(String siteName, Address origin) {
      if (rpcManager.getAddress().equals(rpcManager.getMembers().get(0)) && !rpcManager.getMembers().contains(origin)) {
         stateTransferManager.running().becomeCoordinator(siteName);
      }
   }

   private IntSet localPrimarySegments() {
      return IntSets.from(clusteringDependentLogic.getCacheTopology()
            .getWriteConsistentHash()
            .getPrimarySegmentsForOwner(rpcManager.getAddress()));
   }

   private Flowable<XSiteState> publishDataContainerEntries(IntSet segments) {
      return Flowable.fromIterable(() -> dataContainer.iterator(segments))
            // TODO Investigate removing the filter, we clear L1 entries before becoming an owner
            .filter(NOT_L1_ENTRY)
            .map(XSiteState::fromDataContainer);
   }

   private Flowable<XSiteState> publishStoreEntries(IntSet segments) {
      Publisher<MarshallableEntry<Object, Object>> loaderPublisher =
            persistenceManager.publishEntries(segments, this::missingInDataContainer, true, true,
                  Configurations::isStateTransferStore);
      return Flowable.fromPublisher(loaderPublisher).map(XSiteState::fromCacheLoader);
   }

   private boolean missingInDataContainer(Object key) {
      return !dataContainer.containsKey(key);
   }
}
