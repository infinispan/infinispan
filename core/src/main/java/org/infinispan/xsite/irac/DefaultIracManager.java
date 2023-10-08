package org.infinispan.xsite.irac;

import static org.infinispan.commons.util.IntSets.mutableCopyFrom;
import static org.infinispan.commons.util.IntSets.mutableEmptySet;
import static org.infinispan.remoting.transport.impl.VoidResponseCollector.ignoreLeavers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracCleanupKeysCommand;
import org.infinispan.commands.irac.IracStateResponseCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
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
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ExponentialBackOff;
import org.infinispan.util.ExponentialBackOffImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.commands.remote.IracClearKeysRequest;
import org.infinispan.xsite.commands.remote.IracPutManyRequest;
import org.infinispan.xsite.commands.remote.IracTouchKeyRequest;
import org.infinispan.xsite.commands.remote.XSiteCacheRequest;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableSource;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;

/**
 * Default implementation of {@link IracManager}.
 * <p>
 * It tracks the keys updated by this site and sends them, periodically, to the configured remote sites.
 * <p>
 * The primary owner coordinates everything. It sends the updates request to the remote site and coordinates the local
 * site backup owners. After sending the updates to the remote site, it sends a cleanup request to the local site backup
 * owners
 * <p>
 * The backup owners only keeps a backup list of the tracked keys.
 * <p>
 * On topology change, the updated keys list is replicate to the new owner(s). Also, if a segment is being transferred
 * (i.e. the primary owner isn't a write and read owner), no updates to the remote site is sent since, most likely, the
 * node doesn't have the most up-to-date value.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@MBean(objectName = "AsyncXSiteStatistics", description = "Statistics for Asynchronous cross-site replication")
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracManager implements IracManager, JmxStatisticsExposer {

   private static final Log log = LogFactory.getLog(DefaultIracManager.class);
   private static final Predicate<Map.Entry<Object, IracManagerKeyState>> CLEAR_PREDICATE = e -> {
      e.getValue().discard();
      return true;
   };

   @Inject RpcManager rpcManager;
   @Inject TakeOfflineManager takeOfflineManager;
   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject CommandsFactory commandsFactory;
   @Inject IracTombstoneManager iracTombstoneManager;

   private final Map<Object, IracManagerKeyState> updatedKeys;
   private final Collection<IracXSiteBackup> asyncBackups;
   private final IracExecutor iracExecutor;
   private final int batchSize;
   private volatile boolean hasClear;

   private boolean statisticsEnabled;
   private final LongAdder discardCounts = new LongAdder();
   private final LongAdder conflictLocalWinsCount = new LongAdder();
   private final LongAdder conflictRemoteWinsCount = new LongAdder();
   private final LongAdder conflictMergedCount = new LongAdder();

   public DefaultIracManager(Configuration config, Collection<IracXSiteBackup> backups) {
      updatedKeys = new ConcurrentHashMap<>(64);
      iracExecutor = new IracExecutor(this::run);
      asyncBackups = backups;
      statisticsEnabled = config.statistics().enabled();
      batchSize = config.sites().asyncBackupsStream()
            .map(BackupConfiguration::stateTransfer)
            .mapToInt(XSiteStateTransferConfiguration::chunkSize)
            .reduce(1, Integer::max);
   }

   @Inject
   public void inject(@ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService executorService,
                      @ComponentName(KnownComponentNames.BLOCKING_EXECUTOR) Executor blockingExecutor) {
      // using the inject method here in order to decrease the class size
      iracExecutor.setExecutor(blockingExecutor);
      setBackOff(backup -> new ExponentialBackOffImpl(executorService));
   }

   @Start
   public void start() {
      hasClear = false;
   }

   @Override
   public void trackUpdatedKey(int segment, Object key, Object lockOwner) {
      trackState(new IracManagerKeyChangedState(segment, key, lockOwner, false, asyncBackups.size()));
   }

   @Override
   public void trackExpiredKey(int segment, Object key, Object lockOwner) {
      trackState(new IracManagerKeyChangedState(segment, key, lockOwner, true, asyncBackups.size()));
   }

   @Override
   public CompletionStage<Void> trackForStateTransfer(Collection<XSiteState> stateList) {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Tracking state for state transfer: %s", Util.toStr(stateList));
      }
      // TODO we have way to send only to a single site, we can improve this code!
      AggregateCompletionStage<Void> cf = CompletionStages.aggregateCompletionStage();
      LocalizedCacheTopology topology = clusteringDependentLogic.getCacheTopology();
      for (XSiteState state : stateList) {
         int segment = topology.getSegment(state.key());
         IracManagerStateTransferState iracState = new IracManagerStateTransferState(segment, state.key(), asyncBackups.size());
         // better be safe to avoid losing data
         updatedKeys.put(iracState.getKey(), iracState);
         cf.dependsOn(iracState.getCompletionStage());
      }
      iracExecutor.run();
      return cf.freeze();
   }

   @Override
   public void trackClear(boolean sendClear) {
      // only the originator sends the clear to the backup sites but all nodes need to clear the "updatedKeys" map
      if (log.isTraceEnabled()) {
         log.tracef("Tracking clear request. Replicate to backup sites? %s", sendClear);
      }
      hasClear = sendClear;
      updatedKeys.entrySet().removeIf(CLEAR_PREDICATE);
      if (sendClear) {
         iracExecutor.run();
      }
   }

   @Override
   public void removeState(IracManagerKeyInfo state) {
      removeStateFromLocal(state);
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

      if (!addedSegments.isEmpty()) {
         // request state from old primary owner
         // and group new segments by primary owner
         Map<Address, IntSet> primarySegments = new HashMap<>(newCacheTopology.getMembers().size());
         int numOfSegments = clusteringDependentLogic.getCacheTopology().getNumSegments();
         Function<Address, IntSet> intSetConstructor = address -> mutableEmptySet(numOfSegments);
         for (PrimitiveIterator.OfInt it = addedSegments.iterator(); it.hasNext(); ) {
            int segment = it.nextInt();
            Address primary = newCacheTopology.getWriteConsistentHash().locatePrimaryOwnerForSegment(segment);
            primarySegments.computeIfAbsent(primary, intSetConstructor).set(segment);
         }

         primarySegments.forEach(this::sendStateRequest);
      }

      // even if this node doesn't have any new segments, it may become the primary owner of some segments
      // i.e. backup owner => primary owner "promotion".
      // only trigger a round if we have pending updates.
      if (!updatedKeys.isEmpty()) {
         iracExecutor.run();
      }
   }

   @Override
   public void requestState(Address requestor, IntSet segments) {
      transferStateTo(requestor, segments, updatedKeys.values());
      iracTombstoneManager.sendStateTo(requestor, segments);
   }

   @Override
   public void receiveState(int segment, Object key, Object lockOwner, IracMetadata tombstone) {
      iracTombstoneManager.storeTombstoneIfAbsent(segment, key, tombstone);
      updatedKeys.putIfAbsent(key, new IracManagerKeyChangedState(segment, key, lockOwner, false, asyncBackups.size()));
      iracExecutor.run();
   }

   @Override
   public CompletionStage<Boolean> checkAndTrackExpiration(Object key) {
      if (log.isTraceEnabled()) {
         log.tracef("Checking remote backup sites to see if key %s has been touched recently", key);
      }
      IracTouchKeyRequest command = commandsFactory.buildIracTouchCommand(key);
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
         cmd.add(state.getKeyInfo(), tombstone);
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
         // avoid sending the cleanup command to the cluster members
         old.discard();
      }
      iracExecutor.run();
   }

   private CompletionStage<Void> run() {
      // this run on a blocking thread!
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Sending keys to remote site(s). Is clear? %s, keys: %s", hasClear, Util.toStr(updatedKeys.keySet()));
      }
      if (hasClear) {
         // clear doesn't work very well with concurrent updates
         // let's block all updates until the clear is applied everywhere
         return sendClearUpdate();
      }

      return Flowable.fromIterable(updatedKeys.values())
            .filter(this::canStateBeSent)
            .concatMapMaybe(this::fetchEntry)
            .buffer(batchSize)
            .concatMapCompletable(this::sendUpdateBatch)
            .onErrorComplete(t -> {
               onUnexpectedThrowable(t);
               return true;
            })
            .toCompletionStage(null);
   }

   public void setBackOff(Function<IracXSiteBackup, ExponentialBackOff> builder) {
      asyncBackups.forEach(backup -> backup.useBackOff(builder.apply(backup), iracExecutor));
   }

   public boolean isEmpty() {
      return updatedKeys.isEmpty();
   }

   private boolean canStateBeSent(IracManagerKeyState state) {
      LocalizedCacheTopology cacheTopology = clusteringDependentLogic.getCacheTopology();
      DistributionInfo dInfo = cacheTopology.getSegmentDistribution(state.getSegment());
      //not the primary, we do not send the update to the remote site.
      if (!dInfo.isWriteOwner() && !dInfo.isReadOwner()) {
         // topology changed! we no longer have the key; just drop it
         state.discard();
         removeStateFromLocal(state.getKeyInfo());
         return false;
      }
      return dInfo.isPrimary() && state.canSend();
   }

   private Maybe<IracStateData> fetchEntry(IracManagerKeyState state) {
      return Maybe.fromCompletionStage(clusteringDependentLogic.getEntryLoader()
            .loadAndStoreInDataContainer(state.getKey(), state.getSegment())
            .thenApply(e -> new IracStateData(state, e, iracTombstoneManager.getTombstone(state.getKey())))
            .exceptionally(throwable -> {
               log.debugf(throwable, "[IRAC] Failed to load entry to send to remote sites. It will be retried. State=%s", state);
               state.retry();
               return null;
            }));
   }

   private CompletableSource sendUpdateBatch(Collection<? extends IracStateData> batch) {
      int size = batch.size();
      boolean trace = log.isTraceEnabled();

      if (trace) {
         log.tracef("[IRAC] Batch ready to send remote site with %d keys", size);
      }

      if (size == 0) {
         if (trace) {
            log.trace("[IRAC] Batch not sent, reason: batch is empty");
         }
         return Completable.complete();
      }

      AggregateCompletionStage<Void> aggregation = CompletionStages.aggregateCompletionStage();
      for (IracXSiteBackup backup: asyncBackups) {
         if (backup.isBackOffEnabled()) {
            for (IracStateData data : batch) {
               data.state.retry();
            }
            continue;
         }

         IracPutManyRequest cmd = commandsFactory.buildIracPutManyCommand(size);
         Collection<IracManagerKeyState> invalidState = new ArrayList<>(size);
         Collection<IracManagerKeyState> validState = new ArrayList<>(size);
         for (IracStateData data : batch) {
            if (data.entry == null && data.tombstone == null) {
               // there a concurrency issue where the entry and the tombstone do not exist (remove following by a put)
               // the put will create a new state and the key will be sent to the remote sites
               invalidState.add(data.state);
               continue;
            }

            if (data.state.wasSuccessful(backup))
               continue;

            validState.add(data.state);
            if (data.state.isExpiration()) {
               cmd.addExpire(data.state.getKey(), data.tombstone);
            } else if (data.entry == null) {
               cmd.addRemove(data.state.getKey(), data.tombstone);
            } else {
               cmd.addUpdate(data.state.getKey(), data.entry.getValue(), data.entry.getMetadata(), data.entry.getInternalMetadata().iracMetadata());
            }
         }

         if (!cmd.isEmpty()) {
            IracResponseCollector rspCollector = new IracResponseCollector(commandsFactory.getCacheName(), backup, validState, this::onBatchResponse);
            try {
               if (takeOfflineManager.getSiteState(backup.getSiteName()) != SiteState.OFFLINE) {
                  // The result of this current Completable is not necessary, only the collector's result is.
                  sendToRemoteSite(backup, cmd).whenCompleteAsync(rspCollector, iracExecutor.executor());
                  aggregation.dependsOn(rspCollector);
               } else {
                  // rspCollector is completed, and it is not required to add it to "aggregation"
                  rspCollector.onSiteOffline();
               }
            } catch (Throwable throwable) {
               // safety net; should never happen
               // onUnexpectedThrowable() log the exception!
               onUnexpectedThrowable(throwable);
               for (IracStateData data : batch) {
                  data.state.retry();
               }
            }
         }

         if (!invalidState.isEmpty()) {
            if (trace) {
               log.tracef("[IRAC] Removing %d invalid state(s)", invalidState.size());
            }
            invalidState.forEach(IracManagerKeyState::discard);
            removeStateFromCluster(invalidState);
         }
      }

      return Completable.fromCompletionStage(aggregation.freeze());
   }

   private CompletionStage<Void> sendClearUpdate() {
      // make sure the clear is replicated everywhere before sending the updates!
      IracClearKeysRequest cmd = commandsFactory.buildIracClearKeysCommand();
      IracClearResponseCollector collector = new IracClearResponseCollector(commandsFactory.getCacheName());
      for (IracXSiteBackup backup : asyncBackups) {
         if (takeOfflineManager.getSiteState(backup.getSiteName()) == SiteState.OFFLINE) {
            continue; // backup is offline
         }
         if (backup.isBackOffEnabled()) {
            collector.forceBackOffAndRetry();
            continue;
         }
         collector.dependsOn(backup, sendToRemoteSite(backup, cmd));
      }
      return collector.freeze().handle(this::onClearCompleted);
   }

   private Void onClearCompleted(IracBatchSendResult result, Throwable throwable) {
      if (throwable != null) {
         onUnexpectedThrowable(throwable);
         return null;
      }
      switch (result) {
         case OK:
            hasClear = false;
            // fallthrough
         case RETRY:
         case BACK_OFF_AND_RETRY:
            iracExecutor.run();
            break;
         default:
            onUnexpectedThrowable(new IllegalStateException("Unknown result: " + result));
            break;
      }
      return null;
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

   private <O> XSiteResponse<O> sendToRemoteSite(XSiteBackup backup, XSiteCacheRequest<O> cmd) {
      XSiteResponse<O> rsp = rpcManager.invokeXSite(backup, cmd);
      takeOfflineManager.registerRequest(rsp);
      return rsp;
   }

   private void removeStateFromCluster(Collection<IracManagerKeyState> stateToCleanup) {
      if (stateToCleanup.isEmpty()) {
         return;
      }
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Removing states from cluster: %s", Util.toStr(stateToCleanup));
      }

      IntSet segments = mutableEmptySet();
      LocalizedCacheTopology cacheTopology = clusteringDependentLogic.getCacheTopology();
      Set<Address> owners = new HashSet<>(cacheTopology.getMembers().size());
      List<IracManagerKeyInfo> keysToCleanup = new ArrayList<>(stateToCleanup.size());
      for (IracManagerKeyState state : stateToCleanup) {
         keysToCleanup.add(state.getKeyInfo());
         if (segments.add(state.getSegment())) {
            owners.addAll(cacheTopology.getSegmentDistribution(state.getSegment()).writeOwners());
         }
      }

      if (!segments.isEmpty()) {
         IracCleanupKeysCommand cmd = commandsFactory.buildIracCleanupKeyCommand(keysToCleanup);
         rpcManager.sendToMany(owners, cmd, DeliverOrder.NONE);
         keysToCleanup.forEach(this::removeStateFromLocal);
      }
   }

   private void removeStateFromLocal(IracManagerKeyInfo state) {
      updatedKeys.computeIfPresent(state.getKey(), (ignored, existingState) -> {
         // remove if the same state
         if (existingState.getKeyInfo().equals(state)) {
            if (log.isTraceEnabled()) {
               log.tracef("[IRAC] State removed? true, state=%s", existingState);
            }
            return null;
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("[IRAC] State removed? false, state=%s", existingState);
            }
            return existingState;
         }
      });
   }

   private void onBatchResponse(IracBatchSendResult result, Collection<? extends IracManagerKeyState> successfulSent) {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Batch completed with %d keys applied. Global result=%s", successfulSent.size(), result);
      }
      Collection<IracManagerKeyState> doneKeys = new ArrayList<>(successfulSent.size());
      switch (result) {
         case OK:
            for (IracManagerKeyState state : successfulSent) {
               if (state.isDone()) {
                  doneKeys.add(state);
               }
            }
            break;
         case RETRY:
            iracExecutor.run();
            break;
         case BACK_OFF_AND_RETRY:
            // No-op, backup is already scheduled for retry.
            break;
         default:
            onUnexpectedThrowable(new IllegalStateException("Unknown result: " + result));
            break;
      }
      removeStateFromCluster(doneKeys);
   }

   @ManagedAttribute(description = "Number of keys that need to be sent to remote site(s)",
         displayName = "Queue size",
         measurementType = MeasurementType.DYNAMIC)
   public int getQueueSize() {
      return statisticsEnabled ? updatedKeys.size() : -1;
   }

   @ManagedAttribute(description = "Number of tombstones stored",
         displayName = "Number of tombstones",
         measurementType = MeasurementType.DYNAMIC)
   public int getNumberOfTombstones() {
      return statisticsEnabled ? iracTombstoneManager.size() : -1;
   }

   @ManagedAttribute(description = "The total number of conflicts between local and remote sites.",
         displayName = "Number of conflicts",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfConflicts() {
      return statisticsEnabled ? sumConflicts() : -1;
   }

   @ManagedAttribute(description = "The number of updates from remote sites discarded (duplicate or old update).",
         displayName = "Number of discards",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfDiscards() {
      return statisticsEnabled ? discardCounts.longValue() : -1;
   }

   @ManagedAttribute(description = "The number of conflicts where the merge policy discards the remote update.",
         displayName = "Number of conflicts where local value is used",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfConflictsLocalWins() {
      return statisticsEnabled ? conflictLocalWinsCount.longValue() : -1;
   }

   @ManagedAttribute(description = "The number of conflicts where the merge policy applies the remote update.",
         displayName = "Number of conflicts where remote value is used",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfConflictsRemoteWins() {
      return statisticsEnabled ? conflictRemoteWinsCount.longValue() : -1;
   }

   @ManagedAttribute(description = "Number of conflicts where the merge policy created a new entry.",
         displayName = "Number of conflicts merged",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberOfConflictsMerged() {
      return statisticsEnabled ? conflictMergedCount.longValue() : -1;
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

   @Override
   public Stream<IracManagerKeyInfo> pendingKeys() {
      return updatedKeys.values().stream()
            .map(IracManagerKeyState::getKeyInfo);
   }

   @Override
   public void checkStaleKeys(Address origin, Collection<IracManagerKeyInfo> keys) {
      var topology = clusteringDependentLogic.getCacheTopology();
      var toCleanup = keys.stream()
            .filter(info -> topology.getSegmentDistribution(info.getSegment()).isPrimary())
            .filter(info -> !updatedKeys.containsKey(info.getKey()))
            .toList();
      if (toCleanup.isEmpty()) {
         return;
      }
      IracCleanupKeysCommand cmd = commandsFactory.buildIracCleanupKeyCommand(toCleanup);
      rpcManager.sendTo(origin, cmd, DeliverOrder.NONE);
   }

   private record IracStateData(IracManagerKeyState state, InternalCacheEntry<Object, Object> entry,
                                IracMetadata tombstone) {
      public IracStateData {
         Objects.requireNonNull(state);
      }
   }
}
