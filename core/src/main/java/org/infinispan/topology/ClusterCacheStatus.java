package org.infinispan.topology;

import static org.infinispan.util.logging.Log.CLUSTER;
import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.PersistedConsistentHash;
import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.AvailabilityStrategy;
import org.infinispan.partitionhandling.impl.AvailabilityStrategyContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.RebalanceType;
import org.infinispan.statetransfer.StateTransferTracker;
import org.infinispan.util.concurrent.ConditionFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * Keeps track of a cache's status: members, current/pending consistent hashes, and rebalance status
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class ClusterCacheStatus implements AvailabilityStrategyContext {
   private final ReentrantLock lock = new ReentrantLock();
   // The HotRod client starts with topology 0, so we start with 1 to force an update
   public static final int INITIAL_TOPOLOGY_ID = 1;
   public static final int INITIAL_REBALANCE_ID = 1;

   private static final Log log = LogFactory.getLog(ClusterCacheStatus.class);

   private final EmbeddedCacheManager cacheManager;
   private final GlobalComponentRegistry gcr;
   private final String cacheName;
   private final AvailabilityStrategy availabilityStrategy;
   private final ClusterTopologyManagerImpl clusterTopologyManager;
   private final PersistentUUIDManager persistentUUIDManager;
   private final EventLogger eventLogger;
   private final boolean resolveConflictsOnMerge;
   private final RebalanceType rebalanceType;
   private final Transport transport;
   private final StateTransferTracker stateTransferTracker;

   private int initialTopologyId = INITIAL_TOPOLOGY_ID;
   // Minimal cache clustering configuration
   private volatile CacheJoinInfo joinInfo;
   // Cache members, some of which may not have received state yet
   private volatile List<Address> expectedMembers;
   // Capacity factors for all the members
   private volatile Map<Address, Float> capacityFactors;
   // Cache members that have not yet received state. Always included in the members list.
   private volatile List<Address> joiners;
   // Cache members restoring from graceful shutdown.
   private volatile Set<UUID> restoredMembers;
   // Persistent state (if it exists)
   private final Optional<ScopedPersistentState> persistentState;
   // Cache topology. Its consistent hashes contain only members that did receive/are receiving state
   // The members of both consistent hashes must be included in the members list.
   private volatile CacheTopology currentTopology;
   private volatile CacheTopology stableTopology;
   private volatile AvailabilityMode availabilityMode = AvailabilityMode.AVAILABLE;
   private volatile List<Address> queuedRebalanceMembers;
   private volatile boolean rebalancingEnabled = true;
   private volatile boolean rebalanceInProgress = false;
   private boolean manuallyPutDegraded = false;
   private volatile ConflictResolution conflictResolution;

   private RebalanceConfirmationCollector rebalanceConfirmationCollector;
   private ComponentStatus status;
   private final ConditionFuture<ClusterCacheStatus> hasInitialTopologyFuture;

   public ClusterCacheStatus(EmbeddedCacheManager cacheManager, GlobalComponentRegistry gcr, String cacheName,
                             AvailabilityStrategy availabilityStrategy,
                             RebalanceType rebalanceType, ClusterTopologyManagerImpl clusterTopologyManager,
                             Transport transport, StateTransferTracker stateTransferTracker,
                             PersistentUUIDManager persistentUUIDManager, EventLogManager eventLogManager,
                             Optional<ScopedPersistentState> state, boolean resolveConflictsOnMerge) {
      this.cacheManager = cacheManager;
      this.gcr = gcr;
      this.cacheName = cacheName;
      this.availabilityStrategy = availabilityStrategy;
      this.clusterTopologyManager = clusterTopologyManager;
      this.transport = transport;
      this.stateTransferTracker = stateTransferTracker;
      this.persistentState = state;
      this.resolveConflictsOnMerge = resolveConflictsOnMerge;
      this.rebalanceType = rebalanceType;

      this.currentTopology = null;
      this.stableTopology = null;
      this.expectedMembers = Collections.emptyList();
      this.capacityFactors = Collections.emptyMap();
      this.joiners = Collections.emptyList();
      this.persistentUUIDManager = persistentUUIDManager;
      eventLogger = eventLogManager.getEventLogger().context(cacheName);
      state.ifPresent(scopedPersistentState -> {
         rebalancingEnabled = false;
         availabilityMode = AvailabilityMode.DEGRADED_MODE;
      });
      status = ComponentStatus.INSTANTIATED;
      hasInitialTopologyFuture = new ConditionFuture<>(clusterTopologyManager.timeoutScheduledExecutor);
      if (log.isTraceEnabled()) {
         log.tracef("Cache %s initialized. Persisted state? %s", cacheName, persistentState.isPresent());
      }
   }

   @Override
   public CacheJoinInfo getJoinInfo() {
      return joinInfo;
   }

   @Override
   public List<Address> getExpectedMembers() {
      return expectedMembers;
   }

   @Override
   public void queueRebalance(List<Address> newMembers) {
      acquireLock();
      try {
         if (isRestoringStableTopology()) {
            log.debugf("Postponing rebalance for cache %s, waiting for stable topology confirmations from %s",
                  cacheName, restoredMembers);
            return;
         }

         if (newMembers != null && !newMembers.isEmpty() && totalCapacityFactors() != 0f) {
            log.debugf("Queueing rebalance for cache %s with members %s", cacheName, newMembers);
            queuedRebalanceMembers = newMembers;

            startQueuedRebalance();
         }
      } finally {
         releaseLock();
      }
   }

   @Override
   public Map<Address, Float> getCapacityFactors() {
      return capacityFactors;
   }

   @Override
   public CacheTopology getCurrentTopology() {
      return currentTopology;
   }

   @Override
   public CacheTopology getStableTopology() {
      return stableTopology;
   }

   @Override
   public AvailabilityMode getAvailabilityMode() {
      return availabilityMode;
   }

   @Override
   public void updateAvailabilityMode(List<Address> actualMembers, AvailabilityMode newAvailabilityMode,
                                                   boolean cancelRebalance) {
      acquireLock();
      try {
         AvailabilityMode oldAvailabilityMode = this.availabilityMode;
         boolean modeChanged = setAvailabilityMode(newAvailabilityMode);
         manuallyPutDegraded = false;

         if (modeChanged || !actualMembers.equals(currentTopology.getActualMembers())) {
            ConsistentHash newPendingCH = currentTopology.getPendingCH();
            CacheTopology.Phase newPhase = currentTopology.getPhase();
            if (cancelRebalance) {
               newPendingCH = null;
               newPhase = CacheTopology.Phase.NO_REBALANCE;
               rebalanceConfirmationCollector = null;
            }
            CacheTopology newTopology = new CacheTopology(currentTopology.getTopologyId() + 1,
                    currentTopology.getRebalanceId(), currentTopology.getCurrentCH(), newPendingCH, newPhase, actualMembers, persistentUUIDManager.mapAddresses(actualMembers));
            setCurrentTopology(newTopology);

            CLUSTER.updatingAvailabilityMode(cacheName, oldAvailabilityMode, newAvailabilityMode, newTopology);
            eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheAvailabilityModeChange(
                    newAvailabilityMode, newTopology.getTopologyId()));
            clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, newAvailabilityMode);
         }
      } finally {
         releaseLock();
      }
   }

   @Override
   public void manuallyUpdateAvailabilityMode(List<Address> actualMembers, AvailabilityMode mode, boolean cancelRebalance) {
      acquireLock();
      try {
         updateAvailabilityMode(actualMembers, mode, cancelRebalance);
         this.manuallyPutDegraded = mode == AvailabilityMode.DEGRADED_MODE;
      } finally {
         releaseLock();
      }
   }

   @Override
   public boolean isManuallyDegraded() {
      acquireLock();
      try {
         return manuallyPutDegraded;
      } finally {
         releaseLock();
      }
   }

   @Override
   public void updateTopologiesAfterMerge(CacheTopology currentTopology, CacheTopology stableTopology, AvailabilityMode availabilityMode) {
      acquireLock();
      try {
         Log.CLUSTER.cacheRecoveredAfterMerge(cacheName, currentTopology, availabilityMode);
         eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheRecoveredAfterMerge(
                 currentTopology.getMembers(), currentTopology.getTopologyId()));
         this.currentTopology = currentTopology;
         this.stableTopology = stableTopology;
         this.availabilityMode = availabilityMode;

         if (isRestoringStableTopology()) {
            log.debugf("Skipping broadcast after merge for cache %s, it is restoring from persistent state", cacheName);
            return;
         }

         clusterTopologyManager.broadcastTopologyUpdate(cacheName, currentTopology, availabilityMode);

         if (stableTopology != null) {
            log.updatingStableTopology(cacheName, stableTopology);
            clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, stableTopology);
         }
      } finally {
         releaseLock();
      }
   }

   /**
    * @return {@code true} if the joiner was not already a member, {@code false} otherwise
    */
   @GuardedBy("lock")
   private boolean addMember(Address joiner, CacheJoinInfo joinInfo) {
      if (expectedMembers.contains(joiner)) {
         return false;
      }

      if (this.joinInfo == null) {
         this.joinInfo = joinInfo;
      }

      if (isGracefulStopped())
         return false;

      HashMap<Address, Float> newCapacityFactors = new HashMap<>(capacityFactors);
      newCapacityFactors.put(joiner, joinInfo.getCapacityFactor());
      capacityFactors = Immutables.immutableMapWrap(newCapacityFactors);
      expectedMembers = immutableAdd(expectedMembers, joiner);
      persistentUUIDManager.addPersistentAddressMapping(joiner, joinInfo.getPersistentUUID());
      joiners = immutableAdd(joiners, joiner);
      if (log.isTraceEnabled())
         log.tracef("Added joiner %s to cache %s with persistent uuid %s: members = %s, joiners = %s", joiner, cacheName,
               joinInfo.getPersistentUUID(), expectedMembers, joiners);
      return true;
   }

   /**
    * @return {@code true} if the leaver was a member, {@code false} otherwise
    */
   @GuardedBy("lock")
   private boolean removeMember(Address leaver) {
      if (!expectedMembers.contains(leaver)) {
         if (log.isTraceEnabled()) log.tracef("Trying to remove node %s from cache %s, but it is not a member: " +
               "members = %s", leaver, cacheName, expectedMembers);
         return false;
      }

      expectedMembers = immutableRemove(expectedMembers, leaver);
      HashMap<Address, Float> newCapacityFactors = new HashMap<>(capacityFactors);
      newCapacityFactors.remove(leaver);
      capacityFactors = Immutables.immutableMapWrap(newCapacityFactors);
      joiners = immutableRemove(joiners, leaver);
      if (log.isTraceEnabled()) log.tracef("Removed node %s from cache %s: members = %s, joiners = %s", leaver,
            cacheName, expectedMembers, joiners);
      return true;
   }

   /**
    * @return {@code true} if the members list has changed, {@code false} otherwise
    */
   @GuardedBy("lock")
   private boolean retainMembers(List<Address> newClusterMembers) {
      if (newClusterMembers.containsAll(expectedMembers)) {
         if (log.isTraceEnabled()) log.tracef("Cluster members updated for cache %s, no abrupt leavers detected: " +
               "cache members = %s. Existing members = %s", cacheName, newClusterMembers, expectedMembers);
         return false;
      }

      expectedMembers = immutableRetainAll(expectedMembers, newClusterMembers);
      joiners = immutableRetainAll(joiners, newClusterMembers);
      if (log.isTraceEnabled()) log.tracef("Cluster members updated for cache %s: members = %s, joiners = %s", cacheName,
            expectedMembers, joiners);
      return true;
   }

   @GuardedBy("lock")
   private void setCurrentTopology(CacheTopology newTopology) {
      this.currentTopology = newTopology;

      // update the joiners list
      if (newTopology != null) {
         joiners = immutableRemoveAll(expectedMembers, newTopology.getCurrentCH().getMembers());
         stateTransferTracker.forCache(cacheName).cacheTopologyUpdated(newTopology);
      }
      if (log.isTraceEnabled()) log.tracef("Cache %s topology updated: %s, members = %s, joiners = %s",
            cacheName, currentTopology, expectedMembers, joiners);
      if (newTopology != null) {
         newTopology.logRoutingTableInformation(cacheName);
      }
   }

   @GuardedBy("lock")
   private void setStableTopology(CacheTopology newTopology) {
      this.stableTopology = newTopology;
      if (log.isTraceEnabled()) log.tracef("Cache %s stable topology updated: members = %s, joiners = %s, topology = %s",
            cacheName, expectedMembers, joiners, newTopology);
   }

   private boolean needConsistentHashUpdate() {
      // The list of current members is always included in the list of pending members,
      // so we only need to check one list.
      // Also returns false if both CHs are null
      return !expectedMembers.equals(currentTopology.getMembers());
   }

   private List<Address> pruneInvalidMembers(List<Address> possibleMembers) {
      return immutableRetainAll(possibleMembers, expectedMembers);
   }

   public boolean isRebalanceInProgress() {
      return rebalanceConfirmationCollector != null;
   }

   public RebalancingStatus getRebalancingStatus() {
      if (!isRebalanceEnabled()) {
         return RebalancingStatus.SUSPENDED;
      } else if (rebalanceInProgress) {
         return RebalancingStatus.IN_PROGRESS;
      } else if (queuedRebalanceMembers != null) {
         return RebalancingStatus.PENDING;
      } else {
         return RebalancingStatus.COMPLETE;
      }
   }

   public void confirmRebalancePhase(Address member, int receivedTopologyId) {
      acquireLock();
      try {
         if (currentTopology == null) {
            log.debugf("Ignoring rebalance confirmation from %s for cache %s because the cache has no members",
                    member, cacheName);
            return;
         }
         if (receivedTopologyId < currentTopology.getTopologyId()) {
            log.debugf("Ignoring rebalance confirmation from %s " +
                       "for cache %s because the topology id is old (%d, expected %d)",
                    member, cacheName, receivedTopologyId, currentTopology.getTopologyId());
            return;
         }

         if (rebalanceConfirmationCollector == null) {
            throw new CacheException(String.format("Received invalid rebalance confirmation from %s " +
                                                   "for cache %s, we don't have a rebalance in progress", member, cacheName));
         }

         CLUSTER.rebalancePhaseConfirmedOnNode(currentTopology.getPhase(), cacheName, member, receivedTopologyId);
         rebalanceConfirmationCollector.confirmPhase(member, receivedTopologyId);
      } finally {
         releaseLock();
      }
   }

   /**
    * Should be called after the members list was updated in any other way ({@link #removeMember(Address)},
    * {@link #retainMembers} etc.)
    */
   @GuardedBy("lock")
   private void updateMembers() {
      if (rebalanceConfirmationCollector != null) {
         // We rely on the AvailabilityStrategy updating the current topology beforehand.
         rebalanceConfirmationCollector.updateMembers(currentTopology.getMembers());
      }
   }

   public void doHandleClusterView(int viewId) {
      acquireLock();
      try {
         if (currentTopology == null || isGracefulStopped())
            return;

         List<Address> newClusterMembers = transport.getMembers();
         int newViewId = transport.getViewId();
         if (newViewId != viewId) {
            log.debugf("Cache %s skipping members update for view %d, newer view received: %d",
                    cacheName, viewId, newViewId);
            return;
         }
         if (log.isTraceEnabled()) {
            log.tracef("Cache %s updating members for view %d: %s", cacheName, viewId, newClusterMembers);
         }
         boolean cacheMembersModified = retainMembers(newClusterMembers);
         availabilityStrategy.onClusterViewChange(this, newClusterMembers);

         if (cacheMembersModified) {
            updateMembers();
         }
      } finally {
         releaseLock();
      }
   }

   @GuardedBy("lock")
   private void endRebalance() {
      rebalanceInProgress = false;

      CacheTopology currentTopology = getCurrentTopology();
      if (currentTopology == null) {
         log.tracef("Rebalance finished because there are no more members in cache %s", cacheName);
         return;
      }
      if (rebalanceType != RebalanceType.FOUR_PHASE) {
         throw new IllegalStateException();
      }
      assert currentTopology.getPhase().isRebalance();

      int currentTopologyId = currentTopology.getTopologyId();
      List<Address> members = currentTopology.getMembers();
      var newTopology = new CacheTopology(currentTopologyId + 1, currentTopology.getRebalanceId(),
            currentTopology.getCurrentCH(), currentTopology.getPendingCH(),
            CacheTopology.Phase.READ_ALL_WRITE_ALL, members,
            persistentUUIDManager.mapAddresses(members));

      setCurrentTopology(newTopology);

      if (newTopology.getPhase() != CacheTopology.Phase.NO_REBALANCE) {
         rebalanceConfirmationCollector = new RebalanceConfirmationCollector(cacheName, currentTopologyId + 1,
               members, this::endReadAllPhase);
      } else {
         rebalanceConfirmationCollector = null;
      }
      availabilityStrategy.onRebalanceEnd(this);

      CLUSTER.startingRebalancePhase(cacheName, newTopology);
      eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheRebalancePhaseChange(
         newTopology.getPhase(), newTopology.getTopologyId()));
      // TODO: to members only?
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode);

      if (newTopology.getPhase() == CacheTopology.Phase.NO_REBALANCE) {
         startQueuedRebalance();
      }
   }

   @GuardedBy("lock") // called from doHandleClusterView/doLeave/confirmRebalancePhase
   private void endReadAllPhase() {
      CacheTopology newTopology;
      CacheTopology currentTopology = getCurrentTopology();
      assert currentTopology != null; // can this happen?
      assert currentTopology.getPhase() == CacheTopology.Phase.READ_ALL_WRITE_ALL;

      List<Address> members = currentTopology.getMembers();
      newTopology = new CacheTopology(currentTopology.getTopologyId() + 1, currentTopology.getRebalanceId(),
            currentTopology.getCurrentCH(), currentTopology.getPendingCH(), CacheTopology.Phase.READ_NEW_WRITE_ALL, members,
            persistentUUIDManager.mapAddresses(members));
      setCurrentTopology(newTopology);

      rebalanceConfirmationCollector = new RebalanceConfirmationCollector(cacheName, currentTopology.getTopologyId() + 1,
            members, this::endReadNewPhase);

      CLUSTER.startingRebalancePhase(cacheName, newTopology);
      eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheRebalancePhaseChange(
         newTopology.getPhase(), newTopology.getTopologyId()));
      // TODO: to members only?
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode);
   }

   @GuardedBy("lock") // called from doHandleClusterView/doLeave/confirmRebalancePhase
   private void endReadNewPhase() {
      CacheTopology newTopology;
      CacheTopology currentTopology = getCurrentTopology();
      assert currentTopology != null;
      assert currentTopology.getPhase() == CacheTopology.Phase.READ_NEW_WRITE_ALL;

      List<Address> members = currentTopology.getMembers();
      newTopology = new CacheTopology(currentTopology.getTopologyId() + 1, currentTopology.getRebalanceId(),
            currentTopology.getPendingCH(), null, CacheTopology.Phase.NO_REBALANCE, members,
            persistentUUIDManager.mapAddresses(members));
      setCurrentTopology(newTopology);

      rebalanceConfirmationCollector = null;

      CLUSTER.finishedRebalance(cacheName, newTopology);
      eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.rebalanceFinished(
         newTopology.getMembers(), newTopology.getTopologyId()));
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode);
      startQueuedRebalance();
   }

   // TODO: newMembers isn't really used, pruneInvalidMembers uses expectedMembers
   @Override
   public void updateCurrentTopology(List<Address> newMembers) {
      acquireLock();
      try {
         if (isRestoringStableTopology()) {
            log.debugf("Postponing topology update for cache %s, waiting stable topology install for %s", cacheName, restoredMembers);
            return;
         }
         // The current topology might be null just after a joiner became the coordinator
         if (currentTopology == null) {
            createInitialCacheTopology();
         }
         var consistentHashFactory = getJoinInfo().getConsistentHashFactory();
         int topologyId = currentTopology.getTopologyId();
         int rebalanceId = currentTopology.getRebalanceId();
         ConsistentHash currentCH = currentTopology.getCurrentCH();
         ConsistentHash pendingCH = currentTopology.getPendingCH();
         if (!needConsistentHashUpdate()) {
            log.tracef("Cache %s members list was updated, but the cache topology doesn't need to change: %s",
                    cacheName, currentTopology);
            return;
         }

         if (newMembers.isEmpty()) {
            log.tracef("Cache %s no longer has any members, removing topology", cacheName);
            setCurrentTopology(null);
            setStableTopology(null);
            rebalanceConfirmationCollector = null;
            status = ComponentStatus.INSTANTIATED;
            return;
         }

         if (totalCapacityFactors() == 0f) {
            CLUSTER.debugf("All members have capacity factor 0, delaying topology update");
            return;
         }

         List<Address> newCurrentMembers = pruneInvalidMembers(currentCH.getMembers());
         ConsistentHash newCurrentCH, newPendingCH = null;
         CacheTopology.Phase newPhase = CacheTopology.Phase.NO_REBALANCE;
         List<Address> actualMembers;
         if (newCurrentMembers.isEmpty()) {
            // All the current members left, try to replace them with the joiners
            log.tracef("All current members left, re-initializing status for cache %s", cacheName);
            rebalanceConfirmationCollector = null;

            newCurrentMembers = getExpectedMembers();
            actualMembers = newCurrentMembers;
            newCurrentCH = joinInfo.getConsistentHashFactory().create(joinInfo.getNumOwners(),
                    joinInfo.getNumSegments(), newCurrentMembers, getCapacityFactors());
         } else {
            // ReplicatedConsistentHashFactory allocates segments to all its members, so we can't add any members here
            newCurrentCH = consistentHashFactory.updateMembers(currentCH, newCurrentMembers, getCapacityFactors());
            actualMembers = newCurrentMembers;
            if (pendingCH != null) {
               newPhase = currentTopology.getPhase();
               List<Address> newPendingMembers = pruneInvalidMembers(pendingCH.getMembers());
               newPendingCH = consistentHashFactory.updateMembers(pendingCH, newPendingMembers, getCapacityFactors());
               actualMembers = pruneInvalidMembers(newPendingMembers);
            }
         }
         // Losing members during state transfer could lead to a state where we have more than two topologies
         // concurrently in the cluster. We need to make sure that all the topologies are compatible (properties set
         // in CacheTopology docs hold) - we just remove lost members.
         CacheTopology newTopology = new CacheTopology(topologyId + 1, rebalanceId, newCurrentCH, newPendingCH,
                 newPhase, actualMembers, persistentUUIDManager.mapAddresses(actualMembers));
         setCurrentTopology(newTopology);

         if (rebalanceConfirmationCollector != null) {
            // The node that will cancel the state transfer because of another topology update won't send topology confirm
            log.debugf("Cancelling topology confirmation %s because of another topology update", rebalanceConfirmationCollector);
            rebalanceConfirmationCollector = null;
         }

         CLUSTER.updatingTopology(cacheName, newTopology, availabilityMode);
         eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheMembersUpdated(
                 actualMembers, newTopology.getTopologyId()));
         clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode);
      } finally {
         releaseLock();
      }
   }

   @GuardedBy("lock")
   private float totalCapacityFactors() {
      float totalCapacityFactors = 0f;
      for (Float factor : capacityFactors.values()) {
         totalCapacityFactors += factor;
      }
      return totalCapacityFactors;
   }

   private boolean setAvailabilityMode(AvailabilityMode newAvailabilityMode) {
      if (newAvailabilityMode == availabilityMode)
         return false;

      log.tracef("Cache %s availability changed: %s -> %s", cacheName, availabilityMode, newAvailabilityMode);
      availabilityMode = newAvailabilityMode;
      return true;
   }

   // Helpers for working with immutable lists
   private <T> List<T> immutableAdd(List<T> list, T element) {
      List<T> result = new ArrayList<>(list);
      result.add(element);
      return Collections.unmodifiableList(result);
   }

   private <T> List<T> immutableRemove(List<T> list, T element) {
      List<T> result = new ArrayList<>(list);
      result.remove(element);
      return Collections.unmodifiableList(result);
   }

   private <T> List<T> immutableRemoveAll(List<T> list, List<T> otherList) {
      List<T> result = new ArrayList<>(list);
      result.removeAll(otherList);
      return Collections.unmodifiableList(result);
   }

   private <T> List<T> immutableRetainAll(List<T> list, List<T> otherList) {
      List<T> result = new ArrayList<>(list);
      result.retainAll(otherList);
      return Collections.unmodifiableList(result);
   }

   @Override
   public String toString() {
      return "ClusterCacheStatus{" +
            "cacheName='" + cacheName + '\'' +
            ", members=" + expectedMembers +
            ", joiners=" + joiners +
            ", currentTopology=" + currentTopology +
            ", rebalanceConfirmationCollector=" + rebalanceConfirmationCollector +
            '}';
   }

   public void doMergePartitions(Map<Address, CacheStatusResponse> statusResponses) {
      acquireLock();
      try {
         if (statusResponses.isEmpty()) {
            throw new IllegalArgumentException("Should have at least one current topology");
         }

         HashMap<Address, CacheJoinInfo> joinInfos = new HashMap<>();
         Set<CacheTopology> currentTopologies = new HashSet<>();
         Set<CacheTopology> stableTopologies = new HashSet<>();
         for (Map.Entry<Address, CacheStatusResponse> e : statusResponses.entrySet()) {
            Address sender = e.getKey();
            CacheStatusResponse response = e.getValue();
            // Include nodes that have completed joining or have persistent state from a graceful shutdown.
            // Nodes with persistent state need to be in expectedMembers so the coordinator can restore the topology.
            if (response.getStableTopology() != null || response.getCacheJoinInfo().getPersistentStateChecksum().isPresent())
               joinInfos.put(sender, response.getCacheJoinInfo());
            if (response.getCacheTopology() != null) {
               currentTopologies.add(response.getCacheTopology());
            }
            if (response.getStableTopology() != null) {
               stableTopologies.add(response.getStableTopology());
            }
         }

         log.debugf("Recovered %d partition(s) for cache %s: %s", currentTopologies.size(), cacheName, currentTopologies);
         recoverMembers(joinInfos, currentTopologies, stableTopologies);

         // Recreate and flag the cache in case the cache has a graceful shutdown pending.
         recoverPersistentState(statusResponses);

         // TODO Should automatically detect when the coordinator has left and there is only one partition
         // and continue any in-progress rebalance without resetting the cache topology.

         availabilityStrategy.onPartitionMerge(this, statusResponses);

         // After merge processing, if all restored members are present, attempt topology restoration.
         // This handles the case where a coordinator change during graceful shutdown recovery
         // left some nodes without the restored topology, but all original members are now in the cluster.
         // This avoids completing the whole join cycle again.
         if (isRestoringStableTopology() && status == ComponentStatus.INSTANTIATED) {
            restoreTopologyFromState();
         }
      } catch (IllegalLifecycleStateException e) {
         // Retrieving the conflict manager fails during shutdown, because internalGetCache checks the manager status
         // Remote invocations also fail if the transport is stopped before recovery finishes
      } catch (Exception e) {
         log.failedToRecoverCacheState(cacheName, e);
      } finally {
         releaseLock();
      }
   }

   @GuardedBy("lock")
   private void recoverMembers(Map<Address, CacheJoinInfo> joinInfos,
                               Collection<CacheTopology> currentTopologies, Collection<CacheTopology> stableTopologies) {
      expectedMembers = Collections.emptyList();

      // Try to preserve the member order at least for the first partition
      // TODO First partition is random, it would be better to use the topology selected by the availability strategy
      for (CacheTopology topology : stableTopologies) {
         addMembers(topology.getMembers(), joinInfos);
      }
      for (CacheTopology topology : currentTopologies) {
         addMembers(topology.getMembers(), joinInfos);
      }
      // Add the joiners that are not part of any topology at the end
      for (Map.Entry<Address, CacheJoinInfo> e : joinInfos.entrySet()) {
         if (!expectedMembers.contains(e.getKey())) {
            addMember(e.getKey(), e.getValue());
         }
      }
   }

   @GuardedBy("lock")
   private void recoverPersistentState(Map<Address, CacheStatusResponse> statusResponses) {
      // If none of the nodes has a persistent state set, we can return, there is nothing to restore.
      if (statusResponses.values().stream().map(CacheStatusResponse::getCacheJoinInfo).allMatch(cji -> cji.getPersistentStateChecksum().isEmpty()))
         return;

      // Otherwise, we need to calculate which nodes belong to the previous topology.
      // We might have some different cases:
      //
      // 1. Everything runs correctly, the coordinator installs the stable topology in all nodes, and later a coordinator change.
      // In this case, we consider the cache as recovered, we need to carefully calculate the nodes.
      // If the coordinator change is caused by the previous coordinator leaving, it won't have a status response in the map.
      //
      // 2. There was a problem with the network during the stable topology recovery.
      // In this case, only *some* of the nodes will have a stable topology. The nodes that do not have a stable topology,
      // have not received the stable topology from the coordinator. We need to keep the addresses of the nodes which haven't
      // received the topology until they join the cache.
      //
      // 3. There was a problem with the network and this is a partial restore.
      // If the network was split in multiple partitions (or some nodes have left the cluster), they won't be included in
      // response maps since they are not reachable. We need to be able to distinguish between a node that successfully
      // recovered and left, to a node that didn't recovered and left.
      Set<UUID> members = new HashSet<>();
      Set<UUID> seenStable = new HashSet<>();

      for (CacheStatusResponse csr : statusResponses.values()) {
         CacheJoinInfo cji = csr.getCacheJoinInfo();
         if (cji.getPersistentStateChecksum().isEmpty())
            continue;

         Set<UUID> previous = csr.previousMembers();
         if (previous != null && !previous.isEmpty()) {
            members.addAll(previous);
         }

         CacheTopology topology = csr.getStableTopology();
         if (topology != null)
            seenStable.add(cji.getPersistentUUID());
      }

      members.removeAll(seenStable);

      if (!members.isEmpty()) {
         restoredMembers = members;
         log.debugf("Cache %s has pending members %s to restore stable topology", cacheName, restoredMembers);
      }
   }

   @GuardedBy("lock")
   private void addMembers(Collection<Address> membersToAdd, Map<Address, CacheJoinInfo> joinInfos) {
      for (Address member : membersToAdd) {
         if (!expectedMembers.contains(member)) {
            CacheJoinInfo joinInfo = joinInfos.get(member);
            // Some members of the stable/current topology may not be members any more
            if (joinInfo != null) {
               addMember(member, joinInfo);
            }
         }
      }
   }

   @Override
   public String getCacheName() {
      return cacheName;
   }

   public CacheStatusResponse doJoin(Address joiner, CacheJoinInfo joinInfo) {
      acquireLock();
      try {
         if (persistentState.isPresent()) {
            if (joinInfo.getPersistentStateChecksum().isEmpty()) {
               // The joiner does not have a previous state, the current node has a state, and it is still not recovered!
               // We reply with an empty cache status response so the joiner resends the request later.
               if (status == ComponentStatus.INSTANTIATED) {
                  return CacheStatusResponse.empty();
               }
            } else if (persistentState.get().getChecksum() != joinInfo.getPersistentStateChecksum().get()) {
               throw CLUSTER.nodeWithIncompatibleStateJoiningCache(joiner, cacheName);
            }
         } else {
            // A joiner with state can not join a cache without a state.
            // We only allow this in case the nodes were restoring and a partition happened.
            // A previous participant might have a state and the coordinator don't.
            if (joinInfo.getPersistentStateChecksum().isPresent() && !isRejoiningFromPersistentState(joinInfo)) {
               throw CLUSTER.nodeWithPersistentStateJoiningClusterWithoutState(joiner, cacheName);
            }

            // We also validate the case where the coordinator has already installed the stable topology and deleted the state,
            // but it is still waiting the other nodes confirm the stable topology.
            // In this case, we delay the joiner same as when the node has the persistent state.
            if (joinInfo.getPersistentStateChecksum().isEmpty() && isRestoringStableTopology() && !restoredMembers.contains(joinInfo.getPersistentUUID())) {
               return CacheStatusResponse.empty();
            }
         }

         boolean isFirstMember = getCurrentTopology() == null;
         boolean memberJoined = addMember(joiner, joinInfo);
         if (!memberJoined) {
            if (log.isTraceEnabled()) log.tracef("Trying to add node %s to cache %s, but it is already a member: " +
                                                 "members = %s, joiners = %s, availability = %s", joiner, cacheName, expectedMembers, joiners, availabilityMode);
            // Suppress stableTopology only if it has restored=false during recovery, as that would trigger store clearing on the receiving node.
            // A restored=true stableTopology is safe, it signals data came from persistent state.
            CacheTopology st = isRestoringStableTopology() && stableTopology != null && !stableTopology.wasTopologyRestoredFromState()
                  ? null : stableTopology;
            return new CacheStatusResponse(null, currentTopology, st, availabilityMode, expectedMembers, null);
         }
         final List<Address> current = Collections.unmodifiableList(expectedMembers);
         if (status == ComponentStatus.INSTANTIATED) {
            if (persistentState.isPresent() || isRejoiningFromPersistentState(joinInfo)) {
               if (log.isTraceEnabled()) log.tracef("Node %s joining. Attempting to reform previous cluster", joiner);
               if (restoreTopologyFromState()) {
                  return new CacheStatusResponse(null, currentTopology, stableTopology, availabilityMode, current, null);
               }

               // If the cache has a stable topology pending, we return null for now until all missing nodes join again.
               if (isRestoringStableTopology()) {
                  return new CacheStatusResponse(null, currentTopology, null, availabilityMode, current, null);
               }
            } else {
               if (isFirstMember) {
                  // This node was the first to join. We need to install the initial CH
                  CacheTopology initialTopology = createInitialCacheTopology();

                  // Change our status
                  status = ComponentStatus.RUNNING;

                  // Don't need to broadcast the initial CH update, just return the cache topology to the joiner
                  // But we do need to broadcast the initial topology as the stable topology
                  clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, initialTopology);

                  // Allow nodes with zero capacity that were waiting to join,
                  // but do it on another thread to avoid reentrancy
                  hasInitialTopologyFuture.updateAsync(this, clusterTopologyManager.nonBlockingExecutor);
               }
            }
         }

         CacheTopology topologyBeforeRebalance = getCurrentTopology();
         // Only trigger availability strategy if we have a topology installed
         if (topologyBeforeRebalance != null)
            availabilityStrategy.onJoin(this, joiner);

         return new CacheStatusResponse(null, topologyBeforeRebalance, stableTopology, availabilityMode, current, null);
      } finally {
         releaseLock();
      }
   }

   /**
    * Verify whether a joiner is a member missing from the previous topology.
    *
    * @param cji The joiner information
    * @return {@code true} if the joiner UUID is listed as missing. {@code false}, otherwise.
    */
   @GuardedBy("lock")
   private boolean isRejoiningFromPersistentState(CacheJoinInfo cji) {
      return isRestoringStableTopology() && restoredMembers.contains(cji.getPersistentUUID());
   }

   CompletionStage<Void> nodeCanJoinFuture(CacheJoinInfo joinInfo) {
      if (joinInfo.getCapacityFactor() != 0f || getCurrentTopology() != null)
         return CompletableFutures.completedNull();

      log.tracef("Waiting on initial topology for %s with %s", joinInfo, getCurrentTopology());
      // Creating the initial topology requires at least one node with a non-zero capacity factor
      return hasInitialTopologyFuture.newConditionStage(ccs -> ccs.getCurrentTopology() != null,
                                                        () -> new TimeoutException("Timed out waiting for initial cache topology"),
                                                        joinInfo.getTimeout(), TimeUnit.MILLISECONDS);
   }

   @GuardedBy("lock")
   protected CacheTopology restoreCacheTopology(ScopedPersistentState state) {
      if (log.isTraceEnabled()) log.tracef("Attempting to restore CH for cache %s", cacheName);

      var persistedCH = joinInfo.getConsistentHashFactory().fromPersistentState(state, persistentUUIDManager.persistentUUIDToAddress());
      var ch = persistedCH.consistentHash();
      if (persistedCH.hasMissingMembers() || !getExpectedMembers().containsAll(ch.getMembers())) {
         log.recoverFromStateMissingMembers(cacheName, expectedMembers, persistedCH.totalMembers());
         return null;
      }

      if (getExpectedMembers().size() > ch.getMembers().size()) {
         List<Address> extraneousMembers = new ArrayList<>(getExpectedMembers());
         extraneousMembers.removeAll(ch.getMembers());
         throw CLUSTER.extraneousMembersJoinRestoredCache(extraneousMembers, cacheName);
      }
      int topologyId = currentTopology == null ? initialTopologyId : currentTopology.getTopologyId() + 1;
      restoredMembers = new HashSet<>(persistentUUIDManager.mapAddresses(ch.getMembers()));
      CacheTopology initialTopology = new CacheTopology(topologyId, INITIAL_REBALANCE_ID, true, ch, null,
            CacheTopology.Phase.NO_REBALANCE, ch.getMembers(), persistentUUIDManager.mapAddresses(ch.getMembers()));
      return cacheTopologyCreated(initialTopology);
   }

   @GuardedBy("lock")
   private CacheTopology cacheTopologyCreated(CacheTopology topology) {
      setCurrentTopology(topology);
      setStableTopology(topology);
      rebalancingEnabled = true;
      availabilityMode = AvailabilityMode.AVAILABLE;
      return topology;
   }

   @GuardedBy("lock")
   private boolean restoreTopologyFromState() {
      assert persistentState.isPresent() || (restoredMembers != null && !restoredMembers.isEmpty()) : "Persistent state not available";
      CacheTopology topology;
      // If the persistent state is still present, try restoring from it.
      if (persistentState.isPresent()) {
         topology = restoreCacheTopology(persistentState.get());
      } else {
         // In case the persistent state was cleaned without completing the stable topology restore.
         // We restore the topology once all missing nodes join the cluster again.
         // During this time, all extraneous members will be unable to join.
         // The list of nodes should include only the previous nodes.
         topology = restoreMissingMembers();
      }

      if (topology != null) {
         restoreTopologyFromState(topology);
         return true;
      }
      return false;
   }

   @GuardedBy("lock")
   private CacheTopology restoreMissingMembers() {
      if (stableTopology == null || !isRestoringStableTopology())
         return null;

      Set<UUID> current = expectedMembers.stream()
            .map(persistentUUIDManager.addressToPersistentUUID())
            .collect(Collectors.toSet());

      if (!current.containsAll(restoredMembers)) {
         log.tracef("Cache %s is still missing member exp=%s, curr=%s, rest=%s", cacheName, expectedMembers, current, restoredMembers);
         return null;
      }

      List<Address> oldMembers = stableTopology.getMembers();
      Map<Address, Address> mapper = new HashMap<>();
      List<Address> remappedMembers = new ArrayList<>(oldMembers.size());
      for (Address oldAddr : oldMembers) {
         UUID uuid = persistentUUIDManager.getPersistentUuid(oldAddr);
         Address newAddr = persistentUUIDManager.getAddress(uuid);
         mapper.put(oldAddr, newAddr);
         remappedMembers.add(newAddr);
      }

      ConsistentHash remappedCH = currentTopology.getCurrentCH().transform(addr -> mapper.getOrDefault(addr, addr));
      int topologyId = currentTopology.getTopologyId() + 1;
      CacheTopology topology = new CacheTopology(topologyId, stableTopology.getRebalanceId(), true, remappedCH, null,
            CacheTopology.Phase.NO_REBALANCE, remappedMembers, stableTopology.getMembersPersistentUUIDs());
      cacheTopologyCreated(topology);
      log.debugf("Cache %s restored after missing joins %s", cacheName, topology);

      return topology;
   }

   @GuardedBy("lock")
   private void restoreTopologyFromState(CacheTopology topology) {
      // Change our status
      status = ComponentStatus.RUNNING;
      CLUSTER.updatingTopology(cacheName, topology, availabilityMode);
      eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheMembersUpdated(
          topology.getMembers(), topology.getTopologyId()));
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, topology, availabilityMode);
      clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, topology);
   }

   public void stableTopologyInstallConfirmation(StableTopologyConfirmationCollector.StableTopologyConfirmationResult result) {
      acquireLock();
      try {
         if (result.allNodesConfirmed()) {
            log.debugf("Cache %s all nodes confirmed installing stable topology %s", cacheName, stableTopology);
            restoredMembers = null;

            // Phase 2: only update stable topology to trigger state file deletion.
            // Don't touch currentTopology — preserve the restored CH at id=1.
            int id = stableTopology.getTopologyId() + 1;
            CacheTopology commitStable = new CacheTopology(id, stableTopology.getRebalanceId(), false,
                  stableTopology.getCurrentCH(), null, CacheTopology.Phase.NO_REBALANCE,
                  stableTopology.getMembers(), stableTopology.getMembersPersistentUUIDs());
            setStableTopology(commitStable);
            clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, commitStable);
            return;
         }

         restoredMembers = new HashSet<>(persistentUUIDManager.mapAddresses(List.copyOf(result.pending())));
         log.debugf("Cache %s has unconfirmed nodes %s for stable topology %s", cacheName, restoredMembers, stableTopology);
      } finally {
         releaseLock();
      }
   }

   public boolean setCurrentTopologyAsStable(boolean force) {
      acquireLock();
      try {
         if (currentTopology != null) return false;

         if (persistentState.isPresent()) {
            List<Address> members = getExpectedMembers();
            @SuppressWarnings("unchecked")
            PersistedConsistentHash<?> persistedCH = joinInfo.getConsistentHashFactory()
                  .fromPersistentState(persistentState.get(), persistentUUIDManager.persistentUUIDToAddress());

            // Clean stale mappings: addresses resolved by PersistentUUIDManager but belonging to nodes that left during recovery.
            for (Address chMember : persistedCH.consistentHash().getMembers()) {
               if (!members.contains(chMember)) {
                  persistentUUIDManager.removePersistentAddressMapping(chMember);
               }
            }

            // Re-create with cleaned mappings — stale UUIDs now go into missingUuids.
            persistedCH = joinInfo.getConsistentHashFactory()
                  .fromPersistentState(persistentState.get(), persistentUUIDManager.persistentUUIDToAddress());

            int missing = persistedCH.totalMembers() - members.size();
            int owners = joinInfo.getNumOwners();
            boolean isReplicated = gcr.getCacheManager().getCacheConfiguration(cacheName).clustering().cacheMode().isReplicated();
            if (!isReplicated && !force && missing >= owners) {
               throw log.missingTooManyMembers(cacheName, owners, missing, persistedCH.totalMembers());
            }

            boolean safelyRecovered = missing < owners;
            ConsistentHash ch;
            if (safelyRecovered && !isReplicated) {
               // We reuse the previous topology, only changing it to reflect the current members.
               // This is necessary to keep the same segments mapping as before.
               // If another node joins, it will trigger rebalance, properly redistributing the segments.
               ch = persistedCH.consistentHash();
            } else {
               // We don't have enough members to safely recover the previous topology, so we create a new one, as the
               // node will clear the storage, so we don't need the same segments mapping.
               ConsistentHashFactory<ConsistentHash> chf = joinInfo.getConsistentHashFactory();
               ch = chf.create(joinInfo.getNumOwners(), joinInfo.getNumSegments(), members, getCapacityFactors());
            }
            CacheTopology topology = new CacheTopology(initialTopologyId, INITIAL_REBALANCE_ID, true, ch, null,
                    CacheTopology.Phase.NO_REBALANCE, members, persistentUUIDManager.mapAddresses(members));
            restoredMembers = null;
            restoreTopologyFromState(cacheTopologyCreated(topology));
            return true;
         }

         return false;
      } finally {
         releaseLock();
      }
   }


   @GuardedBy("lock")
   protected CacheTopology createInitialCacheTopology() {
      log.tracef("Initializing status for cache %s", cacheName);
      List<Address> initialMembers = getExpectedMembers();
      ConsistentHash initialCH = joinInfo.getConsistentHashFactory().create(joinInfo.getNumOwners(),
            joinInfo.getNumSegments(), initialMembers, getCapacityFactors());
      CacheTopology initialTopology = new CacheTopology(initialTopologyId, INITIAL_REBALANCE_ID, initialCH, null,
            CacheTopology.Phase.NO_REBALANCE, initialMembers, persistentUUIDManager.mapAddresses(initialMembers));
      setCurrentTopology(initialTopology);
      setStableTopology(initialTopology);
      return initialTopology;
   }

   public CompletionStage<Void> doLeave(Address leaver) {
      acquireLock();
      try {
         boolean actualLeaver = removeMember(leaver);
         if (!actualLeaver)
            return CompletableFutures.completedNull();

         if (expectedMembers.isEmpty())
            clusterTopologyManager.removeCacheStatus(cacheName);

         if (currentTopology == null)
            return CompletableFutures.completedNull();

         availabilityStrategy.onGracefulLeave(this, leaver);

         updateMembers();
         return CompletableFutures.completedNull();
      } finally {
         releaseLock();
      }
   }

   public void startQueuedRebalance() {
      acquireLock();
      try {
         if (isRestoringStableTopology()) {
            log.debugf("Postponing rebalance for cache %s, waiting for stable topology confirmations from %s",
                  cacheName, restoredMembers);
            return;
         }

         // We cannot start rebalance until queued CR is complete
         if (conflictResolution != null) {
            log.tracef("Postponing rebalance for cache %s as conflict resolution is in progress", cacheName);
            return;
         }

         // We don't have a queued rebalance.
         if (queuedRebalanceMembers == null) {
            // The previous topology was not restored. We do nothing, waiting for the members to get back in and install topology.
            if (currentTopology == null && persistentState.isPresent()) {
               log.debugf("Skipping rebalance for cache %s as the previous topology was not restored", cacheName);
               return;
            }

            // We may need to broadcast a stable topology update
            if (stableTopology == null || stableTopology.getTopologyId() < currentTopology.getTopologyId()) {
               stableTopology = currentTopology;
               log.updatingStableTopology(cacheName, stableTopology);
               clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, stableTopology);
            }
            return;
         }

         CacheTopology cacheTopology = getCurrentTopology();
         if (!isRebalanceEnabled()) {
            log.tracef("Postponing rebalance for cache %s, rebalancing is disabled", cacheName);
            return;
         }
         if (rebalanceConfirmationCollector != null) {
            log.tracef("Postponing rebalance for cache %s, there's already a topology change in progress: %s",
                    cacheName, rebalanceConfirmationCollector);
            return;
         }

         if (queuedRebalanceMembers.isEmpty()) {
            log.tracef("Ignoring request to rebalance cache %s, it doesn't have any member", cacheName);
            return;
         }

         if (cacheTopology == null) {
            createInitialCacheTopology();
            return;
         }

         List<Address> newMembers = updateMembersPreservingOrder(cacheTopology.getMembers(), queuedRebalanceMembers);
         queuedRebalanceMembers = null;
         log.tracef("Rebalancing consistent hash for cache %s, members are %s", cacheName, newMembers);

         int newTopologyId = cacheTopology.getTopologyId() + 1;
         int newRebalanceId = cacheTopology.getRebalanceId() + 1;
         ConsistentHash currentCH = cacheTopology.getCurrentCH();
         if (currentCH == null) {
            // There was one node in the cache before, and it left after the rebalance was triggered
            // but before the rebalance actually started.
            log.tracef("Ignoring request to rebalance cache %s, it doesn't have a consistent hash", cacheName);
            return;
         }
         if (!expectedMembers.containsAll(newMembers)) {
            newMembers.removeAll(expectedMembers);
            log.tracef("Ignoring request to rebalance cache %s, we have new leavers: %s", cacheName, newMembers);
            return;
         }

         var chFactory = getJoinInfo().getConsistentHashFactory();
         // This update will only add the joiners to the CH, we have already checked that we don't have leavers
         ConsistentHash updatedMembersCH = chFactory.updateMembers(currentCH, newMembers, getCapacityFactors());
         ConsistentHash balancedCH = chFactory.rebalance(updatedMembersCH);

         boolean removeMembers = !expectedMembers.containsAll(currentCH.getMembers());
         if (removeMembers) {
            // Leavers should have been removed before starting a rebalance, but that might have failed
            // e.g. if all the remaining members had capacity factor 0
            Collection<Address> unwantedMembers = new LinkedList<>(currentCH.getMembers());
            unwantedMembers.removeAll(expectedMembers);
            CLUSTER.debugf("Removing unwanted members from the current consistent hash: %s", unwantedMembers);
            currentCH = updatedMembersCH;
         }

         boolean updateTopology = false;
         boolean rebalance = false;
         boolean updateStableTopology = false;
         if (rebalanceType == RebalanceType.NONE) {
            updateTopology = true;
         } else if (balancedCH.equals(currentCH)) {
            if (log.isTraceEnabled()) log.tracef("The balanced CH is the same as the current CH, not rebalancing");
            updateTopology = cacheTopology.getPendingCH() != null || removeMembers;
            // After a cluster view change that leaves only 1 node, we don't need either a topology update or a rebalance
            // but we must still update the stable topology
            updateStableTopology =
                    cacheTopology.getPendingCH() == null &&
                    (stableTopology == null || cacheTopology.getTopologyId() != stableTopology.getTopologyId());
         } else {
            rebalance = true;
         }

         if (updateTopology) {
            CacheTopology newTopology = new CacheTopology(newTopologyId, cacheTopology.getRebalanceId(), balancedCH, null,
                    CacheTopology.Phase.NO_REBALANCE, balancedCH.getMembers(), persistentUUIDManager.mapAddresses(balancedCH.getMembers()));
            log.tracef("Updating cache %s topology without rebalance: %s", cacheName, newTopology);
            setCurrentTopology(newTopology);

            CLUSTER.updatingTopology(cacheName, newTopology, availabilityMode);
            eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheMembersUpdated(
                    newTopology.getMembers(), newTopology.getTopologyId()));
            clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, getAvailabilityMode());
         } else if (rebalance) {
            CacheTopology.Phase newPhase;
            if (Objects.requireNonNull(rebalanceType) == RebalanceType.FOUR_PHASE) {
               newPhase = CacheTopology.Phase.READ_OLD_WRITE_ALL;
            } else {
               throw new IllegalStateException();
            }
            CacheTopology newTopology = new CacheTopology(newTopologyId, newRebalanceId, currentCH, balancedCH,
                    newPhase, balancedCH.getMembers(), persistentUUIDManager.mapAddresses(balancedCH.getMembers()));
            log.tracef("Updating cache %s topology for rebalance: %s", cacheName, newTopology);
            setCurrentTopology(newTopology);

            rebalanceInProgress = true;
            assert rebalanceConfirmationCollector == null;
            rebalanceConfirmationCollector = new RebalanceConfirmationCollector(cacheName, newTopology.getTopologyId(),
                    newTopology.getMembers(), this::endRebalance);

            CLUSTER.startingRebalancePhase(cacheName, newTopology);
            eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheRebalanceStart(
                    newTopology.getMembers(), newTopology.getPhase(), newTopology.getTopologyId()));
            clusterTopologyManager.broadcastRebalanceStart(cacheName, newTopology);
         } else if (updateStableTopology) {
            stableTopology = currentTopology;
            clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, stableTopology);
         }
      } finally {
         releaseLock();
      }
   }

   private static List<Address> updateMembersPreservingOrder(List<Address> oldMembers, List<Address> newMembers) {
      List<Address> membersPreservingOrder = new ArrayList<>(oldMembers);
      membersPreservingOrder.retainAll(newMembers);
      for (Address a : newMembers) {
         if (!membersPreservingOrder.contains(a)) {
            membersPreservingOrder.add(a);
         }
      }
      return membersPreservingOrder;
   }

   public boolean isRebalanceEnabled() {
      return rebalancingEnabled && clusterTopologyManager.isRebalancingEnabled();
   }

   public CompletionStage<Void> setRebalanceEnabled(boolean enabled) {
      acquireLock();
      try {
         rebalancingEnabled = enabled;
         if (rebalancingEnabled) {
            log.debugf("Rebalancing is now enabled for cache %s", cacheName);
            startQueuedRebalance();
         } else {
            log.debugf("Rebalancing is now disabled for cache %s", cacheName);
         }
         return CompletableFutures.completedNull();
      } finally {
         releaseLock();
      }
   }

   // This method is here to augment with blockhound as we allow it to block, but don't want the calls
   // inside the lock to block - Do not move or rename without updating the reference
   private void acquireLock() {
      lock.lock();
   }

   private void releaseLock() {
      lock.unlock();
   }

   public void forceRebalance() {
      queueRebalance(getCurrentTopology().getMembers());
   }

   /**
    * Verify whether stable topology is still restoring.
    *
    * <p>
    * This execution is triggered in case the cluster is recovering from a graceful shutdown procedure. Until the stable
    * topology is restored, no rebalancing should happen.
    * </p>
    *
    * @return {@code true} if a stable topology is restoring. {@code false}, otherwise.
    */
   @GuardedBy("lock")
   private boolean isRestoringStableTopology() {
      return restoredMembers != null && !restoredMembers.isEmpty();
   }

   public CompletionStage<Void> forceAvailabilityMode(AvailabilityMode newAvailabilityMode) {
      acquireLock();
      try {
         if (currentTopology != null && newAvailabilityMode != availabilityMode) {
            availabilityStrategy.onManualAvailabilityChange(this, newAvailabilityMode);
         }
         return CompletableFutures.completedNull();
      } finally {
         releaseLock();
      }
   }

   public CompletionStage<Void> shutdownCache() {
      acquireLock();
      try {
         if (status == ComponentStatus.RUNNING) {
            status = ComponentStatus.STOPPING;
            availabilityMode = AvailabilityMode.STOPPED;
            CompletionStage<Void> cs = clusterTopologyManager.setRebalancingEnabled(cacheName, false);
            return clusterTopologyManager.broadcastShutdownCache(cacheName)
                    .thenCompose(ignore -> cs)
                    .whenComplete((ignore, t) -> {
                       acquireLock();
                       try {
                          status = ComponentStatus.TERMINATED;
                       } finally {
                          releaseLock();
                       }
                    });
         }
         return CompletableFutures.completedNull();
      } finally {
         releaseLock();
      }
   }

   public void setInitialTopologyId(int initialTopologyId) {
      acquireLock();
      try {
         this.initialTopologyId = initialTopologyId;
      } finally {
         releaseLock();
      }
   }

   @Override
   public boolean resolveConflictsOnMerge() {
      // It doesn't make sense to resolve conflicts if we are not going to rebalance the cache as entries on "old" owners
      // will not be deleted when no rebalance occurs.
      return resolveConflictsOnMerge &&
            cacheManager.getStatus().allowInvocations() &&
            clusterTopologyManager.isRebalancingEnabled() &&
            rebalancingEnabled;
   }

   @Override
   public ConsistentHash calculateConflictHash(ConsistentHash preferredHash, Set<ConsistentHash> distinctHashes,
                                               List<Address> actualMembers) {
      // If we are required to resolveConflicts, then we utilise a union of all distinct CHs. This is necessary
      // to ensure that we read the entries associated with all possible read owners before the rebalance occurs
      var chf = getJoinInfo().getConsistentHashFactory();
      ConsistentHash unionHash = distinctHashes.stream().reduce(preferredHash, chf::union);
      unionHash = chf.union(unionHash, chf.rebalance(unionHash));
      return chf.updateMembers(unionHash, actualMembers, capacityFactors);
   }

   @Override
   public void queueConflictResolution(final CacheTopology conflictTopology, final Set<Address> preferredNodes) {
      acquireLock();
      try {
         if (resolveConflictsOnMerge()) {
            conflictResolution = new ConflictResolution();
            CompletableFuture<Void> resolutionFuture = conflictResolution.queue(conflictTopology, preferredNodes);
            resolutionFuture.thenRun(this::completeConflictResolution);
         }
      } finally {
         releaseLock();
      }
   }

   private void completeConflictResolution() {
      acquireLock();
      try {
         if (log.isTraceEnabled()) log.tracef("Cache %s conflict resolution future complete", cacheName);
         // CR is only queued for PreferConsistencyStrategy when a merge it is determined that the newAvailabilityMode will be AVAILABLE
         // therefore if this method is called we know that the partition must be set to AVAILABLE
         availabilityMode = AvailabilityMode.AVAILABLE;

         // Install a NO_REBALANCE topology with pendingCh == null to signal conflict resolution has finished
         CacheTopology conflictTopology = conflictResolution.topology;
         CacheTopology newTopology = new CacheTopology(conflictTopology.getTopologyId() + 1, conflictTopology.getRebalanceId(), conflictTopology.getCurrentCH(),
                 null, CacheTopology.Phase.NO_REBALANCE, conflictTopology.getActualMembers(), persistentUUIDManager.mapAddresses(conflictTopology.getActualMembers()));

         conflictResolution = null;
         setCurrentTopology(newTopology);
         clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode);

         List<Address> actualMembers = conflictTopology.getActualMembers();
         List<Address> newMembers = getExpectedMembers();
         updateAvailabilityMode(actualMembers, availabilityMode, false);

         // Update the topology to remove leavers - in case there is a rebalance in progress, or rebalancing is disabled
         updateCurrentTopology(newMembers);
         // Then queue a rebalance to include the joiners as well
         queueRebalance(newMembers);
      } finally {
         releaseLock();
      }
   }

   @Override
   public boolean restartConflictResolution(List<Address> members) {
      acquireLock();
      try {
         // If conflictResolution is null then no CR in progress
         if (!resolveConflictsOnMerge() || conflictResolution == null)
            return false;

         // No need to reattempt CR if only one node remains, so cancel CR, cleanup and queue rebalance
         if (members.size() == 1) {
            log.debugf("Cache %s cancelling conflict resolution as only one cluster member: members=%s", cacheName, members);
            conflictResolution.cancelCurrentAttempt();
            conflictResolution = null;
            return false;
         }

         // CR members are the same as newMembers, so no need to restart
         if (!conflictResolution.restartRequired(members)) {
            if (log.isTraceEnabled()) {
               log.tracef("Cache %s not restarting conflict resolution, existing conflict topology contains all members (%s)", cacheName, members);
            }
            return false;
         }

         CacheTopology conflictTopology = conflictResolution.topology;
         var chf = getJoinInfo().getConsistentHashFactory();
         ConsistentHash newHash = chf.updateMembers(conflictTopology.getCurrentCH(), members, capacityFactors);

         conflictTopology = new CacheTopology(currentTopology.getTopologyId() + 1, currentTopology.getRebalanceId(),
                 newHash, null, CacheTopology.Phase.CONFLICT_RESOLUTION, members, persistentUUIDManager.mapAddresses(members));
         currentTopology = conflictTopology;

         log.debugf("Cache %s restarting conflict resolution with topology %s", cacheName, currentTopology);
         clusterTopologyManager.broadcastTopologyUpdate(cacheName, conflictTopology, availabilityMode);

         queueConflictResolution(conflictTopology, conflictResolution.preferredNodes);
         return true;
      } finally {
         releaseLock();
      }
   }

   private void cancelConflictResolutionPhase(CacheTopology resolutionTopology) {
      acquireLock();
      try {
         if (conflictResolution != null) {
            // If the passed topology is not the same as the passed topologyId, then we know that a new
            // ConflictResolution attempt has been queued and therefore we should let this proceed.
            // This check is necessary as it is possible that the call to this method is blocked by
            // a concurrent operation on ClusterCacheStatus that may invalidate the cancel request
            if (conflictResolution.topology.getTopologyId() > resolutionTopology.getTopologyId())
               return;
            completeConflictResolution();
         }
      } finally {
         releaseLock();
      }
   }

   public boolean isGracefulStopped() {
      return availabilityMode == AvailabilityMode.STOPPED || status.isStopping() || status.isTerminated();
   }

   private class ConflictResolution {
      private final ReentrantLock lock = new ReentrantLock();
      final CompletableFuture<Void> future = new CompletableFuture<>();
      final AtomicBoolean cancelledLocally = new AtomicBoolean();
      final InternalConflictManager<?, ?> manager;
      volatile CacheTopology topology;
      volatile Set<Address> preferredNodes;

      // This method is here to augment with blockhound as we allow it to block, but don't want the calls
      // inside the lock to block - Do not move or rename without updating the reference
      private void acquireLock() {
         lock.lock();
      }

      private void releaseLock() {
         lock.unlock();
      }

      ConflictResolution() {
         ComponentRegistry componentRegistry = gcr.getNamedComponentRegistry(cacheName);
         this.manager = componentRegistry.getComponent(InternalConflictManager.class);
      }

      CompletableFuture<Void> queue(CacheTopology topology, Set<Address> preferredNodes) {
         acquireLock();
         try {
            this.topology = topology;
            this.preferredNodes = preferredNodes;

            log.debugf("Cache %s queueing conflict resolution with members %s", cacheName, topology.getMembers());

            Log.CLUSTER.startingConflictResolution(cacheName, currentTopology);
            eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.conflictResolutionStarting(
                    currentTopology.getMembers(), currentTopology.getTopologyId()));

            manager.resolveConflicts(topology, preferredNodes).whenComplete((Void, t) -> {
               if (t == null) {
                  Log.CLUSTER.finishedConflictResolution(cacheName, currentTopology);
                  eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.conflictResolutionFinished(
                          topology.getMembers(), topology.getTopologyId()));
                  future.complete(null);
               } else {
                  // TODO Add log event for cancel/restart
                  if (cancelledLocally.get()) {
                     // We have explicitly cancelled the request, therefore return and do nothing
                     Log.CLUSTER.cancelledConflictResolution(cacheName, topology);
                     eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.conflictResolutionCancelled(
                             topology.getMembers(), topology.getTopologyId()));
                     cancelConflictResolutionPhase(topology);
                  } else if (t instanceof CompletionException) {
                     Throwable cause;
                     Throwable rootCause = t;
                     while ((cause = rootCause.getCause()) != null && (rootCause != cause)) {
                        rootCause = cause;
                     }

                     // TODO When CR fails because a node left the cluster, the new CR can start before we cancel the old one
                     Log.CLUSTER.failedConflictResolution(cacheName, topology, rootCause);
                     eventLogger.error(EventLogCategory.CLUSTER, MESSAGES.conflictResolutionFailed(
                             topology.getMembers(), topology.getTopologyId(), rootCause.getMessage()));
                     // If a node is suspected then we can't restart the CR until a new view is received, so we leave conflictResolution != null
                     // so that on a new view restartConflictResolution can return true
                     if (!(rootCause instanceof SuspectException)) {
                        cancelConflictResolutionPhase(topology);
                     }
                  }
               }
            });
            return future;
         } finally {
            releaseLock();
         }
      }

      void cancelCurrentAttempt() {
         acquireLock();
         try {
            cancelledLocally.set(true);
            manager.cancelConflictResolution();
         } finally {
            releaseLock();
         }
      }

      boolean restartRequired(List<Address> newMembers) {
         acquireLock();
         try {
            assert newMembers != null;
            return !newMembers.equals(topology.getMembers());
         } finally {
            releaseLock();
         }
      }
   }
}
