package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracCleanupKeyCommand;
import org.infinispan.commands.irac.IracTouchKeyCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
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

import net.jcip.annotations.GuardedBy;

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
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracManager implements IracManager {

   private static final Log log = LogFactory.getLog(DefaultIracManager.class);

   @Inject RpcManager rpcManager;
   @Inject TakeOfflineManager takeOfflineManager;
   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject CommandsFactory commandsFactory;
   @Inject IracVersionGenerator iracVersionGenerator;

   private final Map<Object, State> updatedKeys;
   private final Collection<XSiteBackup> asyncBackups;
   private final IracExecutor iracExecutor;
   private volatile boolean hasClear;

   public DefaultIracManager(Configuration config) {
      this.updatedKeys = new ConcurrentHashMap<>();
      this.iracExecutor = new IracExecutor(this::run);
      this.asyncBackups = asyncBackups(config);
   }

   private static Collection<XSiteBackup> asyncBackups(Configuration config) {
      return config.sites().asyncBackupsStream()
            .map(bc -> new XSiteBackup(bc.site(), true, bc.replicationTimeout())) //convert to sync
            .collect(Collectors.toList());
   }

   private static IntSet newIntSet(Address ignored) {
      return IntSets.mutableEmptySet();
   }

   @Inject
   public void inject(@ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService executorService,
                      @ComponentName(KnownComponentNames.BLOCKING_EXECUTOR) Executor blockingExecutor) {
      //using the inject method here in order to decrease the class size
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
      if (log.isTraceEnabled()) {
         log.tracef("Tracking key for %s: %s", lockOwner, key);
      }
      updatedKeys.put(key, new State(segment, key, lockOwner));
      iracExecutor.run();
   }

   @Override
   public CompletionStage<Void> trackForStateTransfer(Collection<XSiteState> stateList) {
      AggregateCompletionStage<Void> cf = CompletionStages.aggregateCompletionStage();
      LocalizedCacheTopology topology = clusteringDependentLogic.getCacheTopology();
      for (XSiteState state : stateList) {
         int segment = topology.getSegment(state.key());
         CompletableState completableState = new CompletableState(segment, state.key(), "state-transfer");
         //if an update is in progress, we don't need to send the same value again.
         if (updatedKeys.putIfAbsent(state.key(), completableState) == null) {
            cf.dependsOn(completableState.completableFuture);
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
      updatedKeys.values().forEach(State::discard);
      iracExecutor.run();
   }

   @Override
   public void cleanupKey(int segment, Object key, Object lockOwner, IracMetadata tombstone) {
      State state = new State(segment, key, lockOwner);
      state.tombstone = tombstone;
      removeStateFromLocal(state);
   }

   @Override
   public void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology) {
      if (log.isTraceEnabled()) {
         log.trace("[IRAC] Topology Updated. Checking pending keys.");
      }
      Address local = rpcManager.getAddress();
      if (!newCacheTopology.getMembers().contains(local)) {
         //not in teh cache topology
         return;
      }
      IntSet addedSegments = IntSets.mutableCopyFrom(newCacheTopology.getWriteConsistentHash().getSegmentsForOwner(local));
      if (oldCacheTopology.getMembers().contains(local)) {
         addedSegments.removeAll(oldCacheTopology.getWriteConsistentHash().getSegmentsForOwner(local));
      }

      if (addedSegments.isEmpty()) {
         //this node doesn't have any new segments but it may become the primary owner of some.
         //trigger a new round (it also removes the keys if the node isn't a owner)
         iracExecutor.run();
         return;
      }

      //group new segments by primary owner
      Map<Address, IntSet> primarySegments = new HashMap<>();
      for (int segment : addedSegments) {
         Address primary = newCacheTopology.getWriteConsistentHash().locatePrimaryOwnerForSegment(segment);
         primarySegments.computeIfAbsent(primary, DefaultIracManager::newIntSet).add(segment);
      }

      primarySegments.forEach(this::sendStateRequest);
      iracExecutor.run();
   }

   @Override
   public void requestState(Address origin, IntSet segments) {
      updatedKeys.values().forEach(state -> sendStateIfNeeded(origin, segments, state.segment, state.key, state.owner));
   }

   @Override
   public void receiveState(int segment, Object key, Object lockOwner, IracMetadata tombstone) {
      iracVersionGenerator.storeTombstoneIfAbsent(key, tombstone);
      updatedKeys.putIfAbsent(key, new State(segment, key, lockOwner));
      iracExecutor.run();
   }

   @Override
   public CompletionStage<Boolean> checkAndTrackExpiration(Object key) {
      if (log.isTraceEnabled()) {
         log.tracef("Checking remote backup sites to see if key %s has been touched recently", key);
      }
      IracTouchKeyCommand command = commandsFactory.buildIracTouchCommand(key);
      AtomicBoolean expired = new AtomicBoolean(true);
      // TODO: technically this waits for all backups to respond - we can optimize so we return early
      // if at least one backup says it isn't expired
      AggregateCompletionStage<AtomicBoolean> collector = CompletionStages.aggregateCompletionStage(expired);
      for (XSiteBackup backup : asyncBackups) {
         if (takeOfflineManager.getSiteState(backup.getSiteName()) == SiteState.OFFLINE) {
            if (log.isTraceEnabled()) {
               log.tracef("Skipping %s as it is offline", backup.getSiteName());
            }
            continue; //backup is offline
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

   //public for testing purposes
   public void sendStateIfNeeded(Address origin, IntSet segments, int segment, Object key, Object lockOwner) {
      if (!segments.contains(segment)) {
         return;
      }
      //send the tombstone too
      IracMetadata tombstone = iracVersionGenerator.getTombstone(key);
      CacheRpcCommand cmd = commandsFactory.buildIracStateResponseCommand(segment, key, lockOwner, tombstone);
      rpcManager.sendTo(origin, cmd, DeliverOrder.NONE);
   }

   private CompletionStage<Void> run() {
      //this run on a blocking thread!
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Sending keys to remote site(s). Has clear? %s, keys: %s", hasClear, updatedKeys.keySet());
      }
      if (hasClear) {
         //clear doesn't work very well with concurrent updates
         //let's block all updates until the clear is applied everywhere
         return sendClearUpdate();
      }

      for (State state : updatedKeys.values()) {
         if (!state.canSend()) {
            continue;
         }
         DistributionInfo dInfo = getDistributionInfo(state.segment);
         if (!dInfo.isPrimary()) {
            state.sendFail();
            continue;
         } else if (!dInfo.isWriteOwner()) {
            //topology changed! cleanup the key
            state.discard();
            continue;
         } else if (!dInfo.isReadOwner()) {
            //state transfer in progress (we are a write owner but not a read owner)
            //we need the data in DataContainer and CacheLoaders, so we must wait until we receive the key or will end up sending a remove update.
            //when the new topology arrives, this will be triggered again
            state.sendFail();
            continue;
         }

         fetchEntry(state.key, dInfo.segmentId())
               .thenApply(lEntry -> lEntry == null ?
                     buildRemoveCommand(state) :
                     commandsFactory.buildIracPutKeyCommand(lEntry))
               .thenAccept(cmd -> {
                  IracResponseCollector rsp = sendCommandToAllBackups(cmd);
                  rsp.whenComplete(state); // this can block in JGroups Flow Control. move to thread pool?
                  rsp.whenComplete(this::onSendingCompleted);
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
      //make sure the clear is replicated everywhere before sending the updates!
      return sendCommandToAllBackups(commandsFactory.buildIracClearKeysCommand())
            .whenComplete(this::onClearCompleted)
            .exceptionally(CompletableFutures.toNullFunction())
            .thenRun(() -> {
            });
   }

   private void onClearCompleted(IracResponseCollector.Result result, Throwable throwable) {
      onRoundCompleted(result, throwable, true);
   }

   private void onSendingCompleted(IracResponseCollector.Result result, Throwable throwable) {
      onRoundCompleted(result, throwable, false);
   }

   private void onRoundCompleted(IracResponseCollector.Result result, Throwable throwable, boolean isClear) {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Round completed (is clear? %s). Result: %s (throwable=%s)", isClear, result, throwable);
      }
      if (throwable != null) {
         //unlikely, retry
         log.unexpectedErrorFromIrac(throwable);
         iracExecutor.run();
         return;
      }
      switch (result) {
         case OK:
            iracExecutor.disableBackOff();
            if (isClear) {
               hasClear = false;
               //re-schedule after clear
               iracExecutor.run();
            }
            return;
         case NETWORK_EXCEPTION:
            iracExecutor.enableBackOff();
            iracExecutor.run();
            return;
         case REMOTE_EXCEPTION:
            //retry
            iracExecutor.disableBackOff();
            iracExecutor.run();
            return;
         default:
            log.unexpectedErrorFromIrac(new IllegalStateException("Unknown result: " + result));
            iracExecutor.run();
      }
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

   private void removeStateFromCluster(State state) {
      if (log.isTraceEnabled()) {
         log.tracef("Replication completed for key '%s'. Lock Owner='%s'", state.key, state.owner);
      }
      //removes the key from the "updated keys map" in all owners.
      DistributionInfo dInfo = getDistributionInfo(state.segment);
      IracCleanupKeyCommand cmd = commandsFactory.buildIracCleanupKeyCommand(state.segment, state.key, state.owner, state.tombstone);
      rpcManager.sendToMany(dInfo.writeOwners(), cmd, DeliverOrder.NONE);
      removeStateFromLocal(state);
   }

   private void removeStateFromLocal(State state) {
      boolean removed = updatedKeys.remove(state.key, state);
      iracVersionGenerator.removeTombstone(state.key, state.tombstone);
      if (log.isTraceEnabled()) {
         log.tracef("Removing key '%s'. LockOwner='%s', removed=%s", state.key, state.tombstone, removed);
      }
   }

   private DistributionInfo getDistributionInfo(int segmentId) {
      return clusteringDependentLogic.getCacheTopology().getSegmentDistribution(segmentId);
   }

   private IracResponseCollector sendCommandToAllBackups(XSiteReplicateCommand<Void> command) {
      assert Objects.nonNull(command);
      IracResponseCollector collector = new IracResponseCollector();
      for (XSiteBackup backup : asyncBackups) {
         if (takeOfflineManager.getSiteState(backup.getSiteName()) == SiteState.OFFLINE) {
            continue; //backup is offline
         }
         collector.dependsOn(sendToRemoteSite(backup, command));
      }
      return collector.freeze();
   }

   private XSiteReplicateCommand<Void> buildRemoveCommand(State state) {
      Object key = state.key;
      IracMetadata metadata = iracVersionGenerator.getTombstone(key);
      if (metadata == null) {
         return null;
      }
      state.tombstone = metadata;
      return commandsFactory.buildIracRemoveKeyCommand(key, metadata);
   }

   private CompletionStage<InternalCacheEntry<Object, Object>> fetchEntry(Object key, int segmentId) {
      return clusteringDependentLogic.getEntryLoader().loadAndStoreInDataContainer(key, segmentId);
   }

   private enum StateStatus {
      NEW,
      SENDING,
      COMPLETED
   }

   private class State implements BiConsumer<IracResponseCollector.Result, Throwable> {
      private final Object key;
      private final Object owner;
      private final int segment;
      @GuardedBy("this")
      StateStatus stateStatus;
      private volatile IracMetadata tombstone;

      private State(int segment, Object key, Object owner) {
         this.segment = segment;
         this.key = key;
         this.owner = owner;
         stateStatus = StateStatus.NEW;
      }

      synchronized boolean canSend() {
         if (log.isTraceEnabled()) {
            log.tracef("[IRAC] State.canSend for key %s (status=%s)", key, stateStatus);
         }
         if (stateStatus == StateStatus.NEW) {
            stateStatus = StateStatus.SENDING;
            return true;
         }
         return false;
      }

      synchronized void discard() {
         if (log.isTraceEnabled()) {
            log.tracef("[IRAC] State.onDiscard for key %s (status=%s)", key, stateStatus);
         }
         stateStatus = StateStatus.COMPLETED;
         removeStateFromLocal(this);
      }

      public synchronized void sendFail() {
         if (log.isTraceEnabled()) {
            log.tracef("[IRAC] State.sendFail for key %s (status=%s)", key, stateStatus);
         }
         if (stateStatus == StateStatus.SENDING) {
            stateStatus = StateStatus.NEW;
         }
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         State state = (State) o;
         return key.equals(state.key) &&
               owner.equals(state.owner);
      }

      @Override
      public int hashCode() {
         return Objects.hash(key, owner);
      }

      @Override
      public void accept(IracResponseCollector.Result result, Throwable throwable) {
         if (throwable != null || result != IracResponseCollector.Result.OK) {
            sendFail();
         } else {
            synchronized (this) {
               //send succeed.
               if (log.isTraceEnabled()) {
                  log.tracef("[IRAC] State.onSuccess for key %s (status=%s)", key, stateStatus);
               }
               if (stateStatus != StateStatus.SENDING) {
                  return;
               }
               stateStatus = StateStatus.COMPLETED;
            }
            removeStateFromCluster(this);
         }
      }
   }

   private class CompletableState extends State {

      private final CompletableFuture<Void> completableFuture;

      private CompletableState(int segment, Object key, Object owner) {
         super(segment, key, owner);
         completableFuture = new CompletableFuture<>();
      }

      @Override
      synchronized void discard() {
         super.discard();
         completableFuture.complete(null);
      }

      @Override
      public void accept(IracResponseCollector.Result result, Throwable throwable) {
         super.accept(result, throwable);
         if (isCompleted()) {
            completableFuture.complete(null);
         }
      }

      synchronized boolean isCompleted() {
         return stateStatus == StateStatus.COMPLETED;
      }
   }
}
