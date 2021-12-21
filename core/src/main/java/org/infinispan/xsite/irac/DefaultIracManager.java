package org.infinispan.xsite.irac;

import static org.infinispan.commons.util.IntSets.mutableCopyFrom;
import static org.infinispan.commons.util.IntSets.mutableEmptySet;
import static org.infinispan.remoting.transport.impl.VoidResponseCollector.ignoreLeavers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracCleanupKeyCommand;
import org.infinispan.commands.irac.IracStateResponseCommand;
import org.infinispan.commands.irac.IracTouchKeyCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ExponentialBackOff;
import org.infinispan.util.ExponentialBackOffImpl;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Default implementation of {@link IracManager}.
 * <p>
 * It tracks the keys updated by this site and sends them, periodically, to the configured remote
 * sites.
 * <p>
 * The primary owner coordinates everything. It sends the updates request to the remote site and
 * coordinates the local site backup owners. After sending the updates to the remote site, it sends
 * a cleanup request to the local site backup owners
 * <p>
 * The backup owners only keeps a backup list of the tracked keys.
 * <p>
 * On topology change, the updated keys list is replicate to the new owner(s). Also, if a segment is
 * being transferred (i.e. the primary owner isn't a write and read owner), no updates to the remote
 * site is sent since, most likely, the node doesn't have the most up-to-date value.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@MBean(objectName = "AsyncXSiteStatistics", description = "Statistics for Asynchronous cross-site replication")
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracManager implements IracManager, JmxStatisticsExposer, IracResponseCollector.IracResponseCompleted {

   private static final Log log = LogFactory.getLog(DefaultIracManager.class);

   @Inject RpcManager rpcManager;
   @Inject TakeOfflineManager takeOfflineManager;
   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject CommandsFactory commandsFactory;
   @Inject IracTombstoneManager iracTombstoneManager;

   private final Map<Object, IracManagerKeyState> updatedKeys;
   private final Collection<XSiteBackup> asyncBackups;
   private final IracExecutor iracExecutor;
   private final int batchSize;
   private volatile boolean hasClear;

   private boolean statisticsEnabled;
   private final LongAdder discardCounts = new LongAdder();
   private final LongAdder conflictLocalWinsCount = new LongAdder();
   private final LongAdder conflictRemoteWinsCount = new LongAdder();
   private final LongAdder conflictMergedCount = new LongAdder();

   public DefaultIracManager(Configuration config) {
      updatedKeys = new ConcurrentHashMap<>(64);
      iracExecutor = new IracExecutor(this::run);
      asyncBackups = asyncBackups(config);
      statisticsEnabled = config.statistics().enabled();
      batchSize = config.sites().asyncBackupsStream()
            .map(BackupConfiguration::stateTransfer)
            .mapToInt(XSiteStateTransferConfiguration::chunkSize)
            .reduce(1, Integer::max);
   }

   public static Collection<XSiteBackup> asyncBackups(Configuration config) {
      return config.sites().asyncBackupsStream()
            .map(bc -> new XSiteBackup(bc.site(), true, bc.replicationTimeout())) //convert to sync
            .collect(Collectors.toList());
   }

   @Inject
   public void inject(@ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService executorService,
                      @ComponentName(KnownComponentNames.BLOCKING_EXECUTOR) Executor blockingExecutor) {
      // using the inject method here in order to decrease the class size
      iracExecutor.setBackOff(new ExponentialBackOffImpl(executorService));
      iracExecutor.setExecutor(blockingExecutor);
   }

   @Start
   public void start() {
      Transport transport = rpcManager.getTransport();
      transport.checkCrossSiteAvailable();
      String localSiteName = transport.localSiteName();
      asyncBackups.removeIf(xSiteBackup -> localSiteName.equals(xSiteBackup.getSiteName()));
      if (log.isTraceEnabled()) {
         String b = asyncBackups.stream().map(XSiteBackup::getSiteName).collect(Collectors.joining(", "));
         log.tracef("Async remote sites found: %s", b);
      }
      hasClear = false;
   }

   @Override
   public void trackUpdatedKey(int segment, Object key, Object lockOwner) {
      trackState(new IracManagerKeyChangedState(segment, key, lockOwner, false));
   }

   @Override
   public void trackExpiredKey(int segment, Object key, Object lockOwner) {
      trackState(new IracManagerKeyChangedState(segment, key, lockOwner, true));
   }

   @Override
   public CompletionStage<Void> trackForStateTransfer(Collection<XSiteState> stateList) {
      AggregateCompletionStage<Void> cf = CompletionStages.aggregateCompletionStage();
      LocalizedCacheTopology topology = clusteringDependentLogic.getCacheTopology();
      for (XSiteState state : stateList) {
         int segment = topology.getSegment(state.key());
         IracManagerStateTransferState iracState = new IracManagerStateTransferState(segment, state.key());
         // if an update is in progress, we don't need to send the same value again.
         if (updatedKeys.putIfAbsent(iracState.getKey(), iracState) == null) {
            cf.dependsOn(iracState.getCompletionStage());
         }
      }
      iracExecutor.run();
      return cf.freeze();
   }

   @Override
   public void trackClear() {
      if (log.isTraceEnabled()) {
         log.trace("Tracking clear request");
      }
      hasClear = true;
      updatedKeys.values().forEach(IracManagerKeyState::discard);
      iracExecutor.run();
   }

   @Override
   public void cleanupKey(int segment, Object key, Object lockOwner) {
      removeStateFromLocal(new IracManagerKeyInfoImpl(segment, key, lockOwner));
   }

   @Override
   public void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology) {
      if (log.isTraceEnabled()) {
         log.trace("[IRAC] Topology Updated. Checking pending keys.");
      }
      Address local = rpcManager.getAddress();
      if (!newCacheTopology.getMembers().contains(local)) {
         // not in teh cache topology
         return;
      }
      IntSet addedSegments = mutableCopyFrom(newCacheTopology.getWriteConsistentHash().getSegmentsForOwner(local));
      if (oldCacheTopology.getMembers().contains(local)) {
         addedSegments.removeAll(oldCacheTopology.getWriteConsistentHash().getSegmentsForOwner(local));
      }

      if (addedSegments.isEmpty()) {
         // this node doesn't have any new segments but it may become the primary owner
         // of some.
         // trigger a new round (it also removes the keys if the node isn't a owner)
         iracExecutor.run();
         return;
      }

      // group new segments by primary owner
      Map<Address, IntSet> primarySegments = new HashMap<>(newCacheTopology.getMembers().size());
         int numOfSegments = clusteringDependentLogic.getCacheTopology().getNumSegments();
         Function<Address, IntSet> intSetConstructor = address -> mutableEmptySet(numOfSegments);
      for (PrimitiveIterator.OfInt it = addedSegments.iterator(); it.hasNext(); ) {
            int segment = it.nextInt();
         Address primary = newCacheTopology.getWriteConsistentHash().locatePrimaryOwnerForSegment(segment);
         primarySegments.computeIfAbsent(primary, intSetConstructor).set(segment);
      }

      primarySegments.forEach(this::sendStateRequest);
      iracExecutor.run();
   }

   @Override
   public void requestState(Address requestor, IntSet segments) {
      transferStateTo(requestor, segments, updatedKeys.values());
      iracTombstoneManager.sendStateTo(requestor, segments);
   }

   @Override
   public void receiveState(int segment, Object key, Object lockOwner, IracMetadata tombstone) {
      iracTombstoneManager.storeTombstoneIfAbsent(segment, key, tombstone);
      updatedKeys.putIfAbsent(key, new IracManagerKeyChangedState(segment, key, lockOwner, false));
      iracExecutor.run();
   }

   @Override
   public CompletionStage<Boolean> checkAndTrackExpiration(Object key) {
      if (log.isTraceEnabled()) {
         log.tracef("Checking remote backup sites to see if key %s has been touched recently", key);
      }
      IracTouchKeyCommand command = commandsFactory.buildIracTouchCommand(key);
      AtomicBoolean expired = new AtomicBoolean(true);
      // TODO: technically this waits for all backups to respond - we can optimize so
      // we return early
      // if at least one backup says it isn't expired
      AggregateCompletionStage<AtomicBoolean> collector = CompletionStages.aggregateCompletionStage(expired);
      for (XSiteBackup backup : asyncBackups) {
         if (takeOfflineManager.getSiteState(backup.getSiteName()) == SiteState.OFFLINE) {
            if (log.isTraceEnabled()) {
               log.tracef("Skipping %s as it is offline", backup.getSiteName());
            }
            continue; // backup is offline
         }
         if (log.isTraceEnabled()) {
            log.tracef("Sending irac touch key command to %s", backup);
         }
         XSiteResponse<Boolean> response = sendToRemoteSite(backup, command);
         collector.dependsOn(response.thenAccept(touched -> {
            if (touched) {
               if (log.isTraceEnabled()) {
                  log.tracef("Key %s was recently touched on a remote site %s", key, backup);
               }
               expired.set(false);
            } else if (log.isTraceEnabled()) {
               log.tracef("Entry %s was expired on remote site %s", key, backup);
            }
         }));
      }
      return collector.freeze().thenApply(AtomicBoolean::get);
   }

   // public for testing purposes
   void transferStateTo(Address dst, IntSet segments, Collection<? extends IracManagerKeyState> stateCollection) {
      if (log.isTraceEnabled()) {
         log.tracef("Starting state transfer to %s. Segments=%s, %s keys to check", dst, segments, stateCollection.size());
      }
      //noinspection ResultOfMethodCallIgnored
      Flowable.fromIterable(stateCollection)
            .filter(s -> !s.isStateTransfer() && !s.isExpiration() && segments.contains(s.getSegment()))
            .buffer(batchSize)
            .concatMapCompletableDelayError(batch -> createAndSendBatch(dst, batch))
            .subscribe(() -> {
               if (log.isTraceEnabled()) {
                  log.tracef("State transfer to %s finished!", dst);
               }
            }, throwable -> {
               if (log.isTraceEnabled()) {
                  log.tracef(throwable, "State transfer to %s failed!", dst);
               }
            });
   }

   private Completable createAndSendBatch(Address dst, Collection<? extends IracManagerKeyState> batch) {
      if (log.isTraceEnabled()) {
         log.tracef("Sending state response to %s. Batch=%s", dst, Util.toStr(batch));
      }
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      ResponseCollector<Void> rspCollector = ignoreLeavers();
      IracStateResponseCommand cmd = commandsFactory.buildIracStateResponseCommand(batch.size());
      for (IracManagerKeyState state : batch) {
         IracMetadata tombstone = iracTombstoneManager.getTombstone(state.getKey());
         cmd.add(state, tombstone);
      }
      return Completable.fromCompletionStage(rpcManager.invokeCommand(dst, cmd, rspCollector, rpcOptions)
            .exceptionally(throwable -> {
               if (log.isTraceEnabled()) {
                  log.tracef(throwable, "Batch sent to %s failed! Batch=%s", dst, Util.toStr(batch));
               }
               return null;
            }));
   }

   private void trackState(IracManagerKeyState state) {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Tracking state %s", state);
      }
      IracManagerKeyState old = updatedKeys.put(state.getKey(), state);
      if (old != null) {
         //avoid sending the cleanup command to the cluster members
         old.discard();
      }
      iracExecutor.run();
   }

   private CompletionStage<Void> run() {
      // this run on a blocking thread!
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Sending keys to remote site(s). Has clear? %s, keys: %s", hasClear, updatedKeys.keySet());
      }
      if (hasClear) {
         // clear doesn't work very well with concurrent updates
         // let's block all updates until the clear is applied everywhere
         return sendClearUpdate();
      }

      for (IracManagerKeyState state : updatedKeys.values()) {
         if (!state.canSend()) {
            continue;
         }
         DistributionInfo dInfo = getDistributionInfo(state.getSegment());
         if (!dInfo.isPrimary()) {
            state.retry();
            continue;
         } else if (!dInfo.isWriteOwner()) {
            // topology changed! cleanup the key
            state.discard();
            continue;
         } else if (!dInfo.isReadOwner()) {
            // state transfer in progress (we are a write owner but not a read owner)
            // we need the data in DataContainer and CacheLoaders, so we must wait until we
            // receive the key or will end up sending a remove update.
            // when the new topology arrives, this will be triggered again
            state.retry();
            continue;
         }

         fetchEntry(state.getKey(), dInfo.segmentId()).thenApply(
               lEntry -> lEntry == null ? buildRemoveCommand(state) : commandsFactory.buildIracPutKeyCommand(lEntry))
               .thenAccept(cmd -> {
                  if (cmd == null) { // only buildRemoveCommand can return null if tombstone is missing
                     log.sendFailMissingTombstone(Util.toStr(state.getKey()));
                     // avoid retrying
                     onResponseCompleted(state, IracResponseCollector.Result.OK);
                     return;
                  }
                  sendCommandToAllBackups(cmd, state, this);
               })
               .exceptionally(throwable -> {
                  state.retry();
                  onUnexpectedThrowable(throwable);
                  return null;
               });

      }
      return CompletableFutures.completedNull();
   }

   public void setBackOff(ExponentialBackOff backOff) {
      iracExecutor.setBackOff(backOff);
   }

   public boolean isEmpty() {
      return updatedKeys.isEmpty();
   }

   private CompletionStage<Void> sendClearUpdate() {
      // make sure the clear is replicated everywhere before sending the updates!
      CompletableFuture<Void> cf = new CompletableFuture<>();
      sendCommandToAllBackups(commandsFactory.buildIracClearKeysCommand(), null, (s, r) -> {
         onRoundCompleted(r, true);
         cf.complete(null);
      });
      return cf;
   }

   private void onRoundCompleted(IracResponseCollector.Result result, boolean isClear) {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Round completed (is clear? %s). Result: %s", isClear, result);
      }
      switch (result) {
         case OK:
            iracExecutor.disableBackOff();
            if (isClear) {
               hasClear = false;
               // re-schedule after clear
               iracExecutor.run();
            }
            return;
         case NETWORK_EXCEPTION:
            iracExecutor.enableBackOff();
            iracExecutor.run();
            return;
         case REMOTE_EXCEPTION:
            // retry
            iracExecutor.disableBackOff();
            iracExecutor.run();
            return;
         default:
            onUnexpectedThrowable(new IllegalStateException("Unknown result: " + result));
      }
   }

   private void onUnexpectedThrowable(Throwable throwable) {
      // unlikely, retry
      log.unexpectedErrorFromIrac(throwable);
      iracExecutor.run();
   }

   private void sendStateRequest(Address primary, IntSet segments) {
      CacheRpcCommand cmd = commandsFactory.buildIracRequestStateCommand(segments);
      rpcManager.sendTo(primary, cmd, DeliverOrder.NONE);
   }

   private <O> XSiteResponse<O> sendToRemoteSite(XSiteBackup backup, XSiteReplicateCommand<O> cmd) {
      XSiteResponse<O> rsp = rpcManager.invokeXSite(backup, cmd);
      takeOfflineManager.registerRequest(rsp);
      return rsp;
   }

   private void removeStateFromCluster(IracManagerKeyInfo state) {
      if (log.isTraceEnabled()) {
         log.tracef("Replication completed for state %s", state);
      }
      // removes the key from the "updated keys map" in all owners.
      DistributionInfo dInfo = getDistributionInfo(state.getSegment());
      IracCleanupKeyCommand cmd = commandsFactory.buildIracCleanupKeyCommand(state.getSegment(), state.getKey(), state.getOwner());
      rpcManager.sendToMany(dInfo.writeOwners(), cmd, DeliverOrder.NONE);
      removeStateFromLocal(state);
   }

   private void removeStateFromLocal(IracManagerKeyInfo state) {
      //noinspection SuspiciousMethodCalls
      boolean removed = updatedKeys.remove(state.getKey(), state);
      if (log.isTraceEnabled()) {
         log.tracef("Removing state '%s'. removed=%s", state, removed);
      }
   }

   private DistributionInfo getDistributionInfo(int segmentId) {
      return clusteringDependentLogic.getCacheTopology().getSegmentDistribution(segmentId);
   }

   private void sendCommandToAllBackups(XSiteReplicateCommand<Void> command, IracManagerKeyState state, IracResponseCollector.IracResponseCompleted notifier) {
      assert Objects.nonNull(command);
      IracResponseCollector collector = new IracResponseCollector(state, notifier);
      for (XSiteBackup backup : asyncBackups) {
         if (takeOfflineManager.getSiteState(backup.getSiteName()) == SiteState.OFFLINE) {
            continue; // backup is offline
         }
         collector.dependsOn(backup, sendToRemoteSite(backup, command));
      }
      collector.freeze();
   }

   private XSiteReplicateCommand<Void> buildRemoveCommand(IracManagerKeyState state) {
      IracMetadata metadata = iracTombstoneManager.getTombstone(state.getKey());
      if (metadata == null) {
         return null;
      }
      return commandsFactory.buildIracRemoveKeyCommand(state.getKey(), metadata, state.isExpiration());
   }

   private CompletionStage<InternalCacheEntry<Object, Object>> fetchEntry(Object key, int segmentId) {
      return clusteringDependentLogic.getEntryLoader().loadAndStoreInDataContainer(key, segmentId);
   }

   @Override
   public void onResponseCompleted(IracManagerKeyState state, IracResponseCollector.Result result) {
      if (result == IracResponseCollector.Result.OK && state.done()) {
         removeStateFromCluster(state);
      } else {
         state.retry();
      }
      onRoundCompleted(result, false);
   }

   @ManagedAttribute(description = "Number of keys that need to be sent to remote site(s)",
         displayName = "Queue size",
         measurementType = MeasurementType.DYNAMIC)
   public int getQueueSize() {
      return getStatisticsEnabled() ? updatedKeys.size() : -1;
   }

   @ManagedAttribute(description = "Number of tombstones stored",
         displayName = "Number of tombstones",
         measurementType = MeasurementType.DYNAMIC)
   public int getNumberOfTombstones() {
      return getStatisticsEnabled() ? iracTombstoneManager.size() : -1;
   }

   @ManagedAttribute(description = "The total number of conflicts between local and remote sites.",
         displayName = "Number of conflicts",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfConflicts() {
      return getStatisticsEnabled() ? sumConflicts() : -1;
   }

   @ManagedAttribute(description = "The number of updates from remote sites discarded (duplicate or old update).",
         displayName = "Number of discards",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfDiscards() {
      return getStatisticsEnabled() ? discardCounts.longValue() : -1;
   }

   @ManagedAttribute(description = "The number of conflicts where the merge policy discards the remote update.",
         displayName = "Number of conflicts where local value is used",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfConflictsLocalWins() {
      return getStatisticsEnabled() ? conflictLocalWinsCount.longValue() : -1;
   }

   @ManagedAttribute(description = "The number of conflicts where the merge policy applies the remote update.",
         displayName = "Number of conflicts where remote value is used",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfConflictsRemoteWins() {
      return getStatisticsEnabled() ? conflictRemoteWinsCount.longValue() : -1;
   }

   @ManagedAttribute(description = "Number of conflicts where the merge policy created a new entry.",
         displayName = "Number of conflicts merged",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfConflictsMerged() {
      return getStatisticsEnabled() ? conflictMergedCount.longValue() : -1;
   }

   @ManagedAttribute(description = "Is tombstone cleanup task running?",
         displayName = "Tombstone cleanup task running",
         dataType = DataType.TRAIT)
   public boolean isTombstoneCleanupTaskRunning() {
      return iracTombstoneManager.isTaskRunning();
   }

   @ManagedAttribute(description = "Current delay in milliseconds between tombstone cleanup tasks",
         displayName = "Delay between tombstone cleanup tasks",
         measurementType = MeasurementType.DYNAMIC)
   public long getTombstoneCleanupTaskCurrentDelay() {
      return iracTombstoneManager.getCurrentDelayMillis();
   }

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
   @Override
   public boolean getStatisticsEnabled() {
      return statisticsEnabled;
   }

   /**
    * @param enabled whether gathering statistics for JMX are enabled.
    */
   @Override
   public void setStatisticsEnabled(boolean enabled) {
      statisticsEnabled = enabled;
   }

   /**
    * Resets statistics gathered. Is a no-op, and should be overridden if it is to be meaningful.
    */
   @ManagedOperation(displayName = "Reset Statistics", description = "Resets statistics gathered by this component")
   @Override
   public void resetStatistics() {
      discardCounts.reset();
      conflictLocalWinsCount.reset();
      conflictRemoteWinsCount.reset();
      conflictMergedCount.reset();
   }

   private long sumConflicts() {
      return conflictLocalWinsCount.longValue() + conflictRemoteWinsCount.longValue() + conflictMergedCount.longValue();
   }

   @Override
   public void incrementNumberOfDiscards() {
      discardCounts.increment();
   }

   @Override
   public void incrementNumberOfConflictLocalWins() {
      conflictLocalWinsCount.increment();
   }

   @Override
   public void incrementNumberOfConflictRemoteWins() {
      conflictRemoteWinsCount.increment();
   }

   @Override
   public void incrementNumberOfConflictMerged() {
      conflictMergedCount.increment();
   }

   @Override
   public boolean containsKey(Object key) {
      return updatedKeys.containsKey(key);
   }
}
