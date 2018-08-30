package org.infinispan.topology;

import static org.infinispan.util.logging.LogFactory.CLUSTER;
import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.AdvancedCache;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Immutables;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.conflict.impl.DefaultConflictManager;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

import net.jcip.annotations.GuardedBy;

/**
 * Keeps track of a cache's status: members, current/pending consistent hashes, and rebalance status
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class ClusterCacheStatus implements AvailabilityStrategyContext {
   // The HotRod client starts with topology 0, so we start with 1 to force an update
   public static final int INITIAL_TOPOLOGY_ID = 1;
   public static final int INITIAL_REBALANCE_ID = 1;

   private static final Log log = LogFactory.getLog(ClusterCacheStatus.class);
   private static boolean trace = log.isTraceEnabled();

   private final EmbeddedCacheManager cacheManager;
   private final String cacheName;
   private final AvailabilityStrategy availabilityStrategy;
   private final ClusterTopologyManager clusterTopologyManager;
   private final PersistentUUIDManager persistentUUIDManager;
   private EventLogger eventLogger;
   private final boolean resolveConflictsOnMerge;
   private final RebalanceType rebalanceType;
   private Transport transport;

   private int initialTopologyId = INITIAL_TOPOLOGY_ID;
   // Minimal cache clustering configuration
   private volatile CacheJoinInfo joinInfo;
   // Cache members, some of which may not have received state yet
   private volatile List<Address> expectedMembers;
   // Capacity factors for all the members
   private volatile Map<Address, Float> capacityFactors;
   // Cache members that have not yet received state. Always included in the members list.
   private volatile List<Address> joiners;
   // Persistent state (if it exists)
   private Optional<ScopedPersistentState> persistentState;
   // Cache topology. Its consistent hashes contain only members that did receive/are receiving state
   // The members of both consistent hashes must be included in the members list.
   private volatile CacheTopology currentTopology;
   private volatile CacheTopology stableTopology;
   private volatile AvailabilityMode availabilityMode = AvailabilityMode.AVAILABLE;
   private volatile List<Address> queuedRebalanceMembers;
   private volatile boolean rebalancingEnabled = true;
   private volatile boolean rebalanceInProgress = false;
   private volatile ConflictResolution conflictResolution;

   private RebalanceConfirmationCollector rebalanceConfirmationCollector;
   private ComponentStatus status;

   public ClusterCacheStatus(EmbeddedCacheManager cacheManager, String cacheName,
                             AvailabilityStrategy availabilityStrategy,
                             RebalanceType rebalanceType, ClusterTopologyManager clusterTopologyManager,
                             Transport transport,
                             PersistentUUIDManager persistentUUIDManager, EventLogManager eventLogManager,
                             Optional<ScopedPersistentState> state, boolean resolveConflictsOnMerge) {
      this.cacheManager = cacheManager;
      this.cacheName = cacheName;
      this.availabilityStrategy = availabilityStrategy;
      this.clusterTopologyManager = clusterTopologyManager;
      this.transport = transport;
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
      if (trace) {
         log.tracef("Cache %s initialized. Persisted state? %s", cacheName, persistentState.isPresent());
      }
   }

   public CacheJoinInfo getJoinInfo() {
      return joinInfo;
   }

   @Override
   public List<Address> getExpectedMembers() {
      return expectedMembers;
   }

   @Override
   public synchronized void queueRebalance(List<Address> newMembers) {
      if (newMembers != null && !newMembers.isEmpty()) {
         log.debugf("Queueing rebalance for cache %s with members %s", cacheName, newMembers);
         queuedRebalanceMembers = newMembers;
         startQueuedRebalance();
      }
   }

   public boolean isTotalOrder() {
      return joinInfo.isTotalOrder();
   }

   public boolean isDistributed() {
      return joinInfo.getCacheMode().isDistributed();
   }

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
   public synchronized void updateAvailabilityMode(List<Address> actualMembers, AvailabilityMode newAvailabilityMode,
                                                   boolean cancelRebalance) {
      AvailabilityMode oldAvailabilityMode = this.availabilityMode;
      boolean modeChanged = setAvailabilityMode(newAvailabilityMode);

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
         clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, newAvailabilityMode,
               isTotalOrder(), isDistributed());
      }
   }

   @Override
   public synchronized void updateTopologiesAfterMerge(CacheTopology currentTopology, CacheTopology stableTopology, AvailabilityMode availabilityMode) {
      LogFactory.CLUSTER.cacheRecoveredAfterMerge(cacheName, currentTopology, availabilityMode);
      eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheRecoveredAfterMerge(
         currentTopology.getMembers(), currentTopology.getTopologyId()));
      this.currentTopology = currentTopology;
      this.stableTopology = stableTopology;
      this.availabilityMode = availabilityMode;

      if (currentTopology != null) {
         clusterTopologyManager.broadcastTopologyUpdate(cacheName, currentTopology, availabilityMode, isTotalOrder(), isDistributed());
      }

      if (stableTopology != null) {
         log.updatingStableTopology(cacheName, stableTopology);
         clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, stableTopology, isTotalOrder(),
               isDistributed());
      }
   }

   /**
    * @return {@code true} if the joiner was not already a member, {@code false} otherwise
    */
   @GuardedBy("this")
   private boolean addMember(Address joiner, CacheJoinInfo joinInfo) {
      if (expectedMembers.contains(joiner)) {
         return false;
      }

      // Validate if the member is allowed to join
      if (persistentState.isPresent()) {
         if (!joinInfo.getPersistentStateChecksum().isPresent()) {
            if (status == ComponentStatus.INSTANTIATED) {
               throw log.nodeWithoutPersistentStateJoiningCacheWithState(joiner, cacheName);
            }
         } else if (persistentState.get().getChecksum() != joinInfo.getPersistentStateChecksum().get()) {
            throw log.nodeWithIncompatibleStateJoiningCache(joiner, cacheName);
         }
      } else {
         if (joinInfo.getPersistentStateChecksum().isPresent()) {
            throw log.nodeWithPersistentStateJoiningClusterWithoutState(joiner, cacheName);
         }
      }

      if (this.joinInfo == null) {
         this.joinInfo = joinInfo;
      }

      HashMap<Address, Float> newCapacityFactors = new HashMap<Address, Float>(capacityFactors);
      newCapacityFactors.put(joiner, joinInfo.getCapacityFactor());
      capacityFactors = Immutables.immutableMapWrap(newCapacityFactors);
      expectedMembers = immutableAdd(expectedMembers, joiner);
      persistentUUIDManager.addPersistentAddressMapping(joiner, joinInfo.getPersistentUUID());
      joiners = immutableAdd(joiners, joiner);
      if (trace)
         log.tracef("Added joiner %s to cache %s with persistent uuid %s: members = %s, joiners = %s", joiner, cacheName,
               joinInfo.getPersistentUUID(), expectedMembers, joiners);
      return true;
   }

   /**
    * @return {@code true} if the leaver was a member, {@code false} otherwise
    */
   @GuardedBy("this")
   private boolean removeMember(Address leaver) {
      if (!expectedMembers.contains(leaver)) {
         if (trace) log.tracef("Trying to remove node %s from cache %s, but it is not a member: " +
               "members = %s", leaver, cacheName, expectedMembers);
         return false;
      }

      expectedMembers = immutableRemove(expectedMembers, leaver);
      HashMap<Address, Float> newCapacityFactors = new HashMap<Address, Float>(capacityFactors);
      newCapacityFactors.remove(leaver);
      capacityFactors = Immutables.immutableMapWrap(newCapacityFactors);
      joiners = immutableRemove(joiners, leaver);
      if (trace) log.tracef("Removed node %s from cache %s: members = %s, joiners = %s", leaver,
            cacheName, expectedMembers, joiners);
      return true;
   }

   /**
    * @return {@code true} if the members list has changed, {@code false} otherwise
    */
   @GuardedBy("this")
   private boolean retainMembers(List<Address> newClusterMembers) {
      if (newClusterMembers.containsAll(expectedMembers)) {
         if (trace) log.tracef("Cluster members updated for cache %s, no abrupt leavers detected: " +
               "cache members = %s. Existing members = %s", cacheName, newClusterMembers, expectedMembers);
         return false;
      }

      expectedMembers = immutableRetainAll(expectedMembers, newClusterMembers);
      joiners = immutableRetainAll(joiners, newClusterMembers);
      if (trace) log.tracef("Cluster members updated for cache %s: members = %s, joiners = %s", cacheName,
            expectedMembers, joiners);
      return true;
   }

   @GuardedBy("this")
   private void setCurrentTopology(CacheTopology newTopology) {
      this.currentTopology = newTopology;

      // update the joiners list
      if (newTopology != null) {
         joiners = immutableRemoveAll(expectedMembers, newTopology.getCurrentCH().getMembers());
      }
      if (trace) log.tracef("Cache %s topology updated: %s, members = %s, joiners = %s",
            cacheName, currentTopology, expectedMembers, joiners);
      if (newTopology != null) {
         newTopology.logRoutingTableInformation();
      }
   }

   @GuardedBy("this")
   private void setStableTopology(CacheTopology newTopology) {
      this.stableTopology = newTopology;
      if (trace) log.tracef("Cache %s stable topology updated: members = %s, joiners = %s, topology = %s",
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

   public synchronized void confirmRebalancePhase(Address member, int receivedTopologyId) throws Exception {
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

      LogFactory.CLUSTER.rebalancePhaseConfirmedOnNode(currentTopology.getPhase(), cacheName, member,
                                                       receivedTopologyId);
      rebalanceConfirmationCollector.confirmPhase(member, receivedTopologyId);
   }

   /**
    * Should be called after the members list was updated in any other way ({@link #removeMember(Address)},
    * {@link #retainMembers} etc.)
    *
    * @return {@code true} if the rebalance was confirmed with this update, {@code false} if more confirmations are
    * needed or if the rebalance was already confirmed in another way (e.g. the last member confirmed)
    */
   @GuardedBy("this")
   private void updateMembers() {
      if (rebalanceConfirmationCollector != null) {
         // We rely on the AvailabilityStrategy updating the current topology beforehand.
         rebalanceConfirmationCollector.updateMembers(currentTopology.getMembers());
      }
   }

   public synchronized void doHandleClusterView() {
      // TODO Clean up ClusterCacheStatus instances once they no longer have any members
      if (currentTopology == null)
         return;

      List<Address> newClusterMembers = transport.getMembers();
      boolean cacheMembersModified = retainMembers(newClusterMembers);
      if (trace) log.tracef("Cache %s doHandleClusterView newClusterMembers=%s", cacheName, newClusterMembers);
      availabilityStrategy.onClusterViewChange(this, newClusterMembers);

      if (cacheMembersModified) {
         updateMembers();
      }
   }

   @GuardedBy("this") // called from doHandleClusterView/doLeave/confirmRebalancePhase
   private void endRebalance() {
      CacheTopology newTopology;
      rebalanceInProgress = false;

      CacheTopology currentTopology = getCurrentTopology();
      if (currentTopology == null) {
         log.tracef("Rebalance finished because there are no more members in cache %s", cacheName);
         return;
      }
      assert currentTopology.getPhase().isRebalance();

      int currentTopologyId = currentTopology.getTopologyId();
      List<Address> members = currentTopology.getMembers();
      switch (rebalanceType) {
         case FOUR_PHASE:
            newTopology = new CacheTopology(currentTopologyId + 1, currentTopology.getRebalanceId(),
                  currentTopology.getCurrentCH(), currentTopology.getPendingCH(),
                  CacheTopology.Phase.READ_ALL_WRITE_ALL, members,
                  persistentUUIDManager.mapAddresses(members));
            break;
         case TWO_PHASE:
            newTopology = new CacheTopology(currentTopologyId + 1, currentTopology.getRebalanceId(),
                  currentTopology.getPendingCH(), null,
                  CacheTopology.Phase.NO_REBALANCE, members,
                  persistentUUIDManager.mapAddresses(members));
            break;
         default:
            throw new IllegalStateException();
      }

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
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode,
            isTotalOrder(), isDistributed());

      if (newTopology.getPhase() == CacheTopology.Phase.NO_REBALANCE) {
         startQueuedRebalance();
      }
   }

   @GuardedBy("this") // called from doHandleClusterView/doLeave/confirmRebalancePhase
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
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode, isTotalOrder(), isDistributed());
   }

   @GuardedBy("this") // called from doHandleClusterView/doLeave/confirmRebalancePhase
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
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode, isTotalOrder(), isDistributed());
      startQueuedRebalance();
   }

   // TODO: newMembers isn't really used, pruneInvalidMembers uses expectedMembers
   @Override
   public synchronized void updateCurrentTopology(List<Address> newMembers) {
      // The current topology might be null just after a joiner became the coordinator
      if (currentTopology == null) {
         createInitialCacheTopology();
      }
      ConsistentHashFactory<ConsistentHash> consistentHashFactory = getJoinInfo().getConsistentHashFactory();
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
         newCurrentCH = joinInfo.getConsistentHashFactory().create(
               joinInfo.getHashFunction(), joinInfo.getNumOwners(), joinInfo.getNumSegments(),
               newCurrentMembers, getCapacityFactors());
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
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode,
            isTotalOrder(), isDistributed());
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
      List<T> result = new ArrayList<T>(list);
      result.add(element);
      return Collections.unmodifiableList(result);
   }

   private <T> List<T> immutableRemove(List<T> list, T element) {
      List<T> result = new ArrayList<T>(list);
      result.remove(element);
      return Collections.unmodifiableList(result);
   }

   private <T> List<T> immutableRemoveAll(List<T> list, List<T> otherList) {
      List<T> result = new ArrayList<T>(list);
      result.removeAll(otherList);
      return Collections.unmodifiableList(result);
   }

   private <T> List<T> immutableRetainAll(List<T> list, List<T> otherList) {
      List<T> result = new ArrayList<T>(list);
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

   public synchronized void doMergePartitions(Map<Address, CacheStatusResponse> statusResponses) {
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

         // TODO Should automatically detect when the coordinator has left and there is only one partition
         // and continue any in-progress rebalance without resetting the cache topology.

         availabilityStrategy.onPartitionMerge(this, statusResponses);
      } catch (IllegalLifecycleStateException e) {
         // Retrieving the conflict manager fails during shutdown, because internalGetCache checks the manager status
         // Remote invocations also fail if the transport is stopped before recovery finishes
      } catch (Exception e) {
         log.failedToRecoverCacheState(cacheName, e);
      }
   }

   @GuardedBy("this")
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

   @GuardedBy("this")
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

   public synchronized CacheStatusResponse doJoin(Address joiner, CacheJoinInfo joinInfo) throws Exception {
      boolean isFirstMember = getCurrentTopology() == null;
      boolean memberJoined = addMember(joiner, joinInfo);
      if (!isFirstMember && !memberJoined) {
         if (trace) log.tracef("Trying to add node %s to cache %s, but it is already a member: " +
               "members = %s, joiners = %s", joiner, cacheName, expectedMembers, joiners);
         return new CacheStatusResponse(null, currentTopology, stableTopology, availabilityMode);
      }
      if (status == ComponentStatus.INSTANTIATED) {
         if (persistentState.isPresent()) {
            if (trace) log.tracef("Node %s joining. Attempting to reform previous cluster", joiner);
            // We can only allow this to proceed if we have a complete cluster
            CacheTopology topology = restoreCacheTopology(persistentState.get());
            if (topology != null) {
               // Change our status
               status = ComponentStatus.RUNNING;
               CLUSTER.updatingTopology(cacheName, topology, availabilityMode);
               eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.cacheMembersUpdated(
                  topology.getMembers(), topology.getTopologyId()));
               clusterTopologyManager.broadcastTopologyUpdate(cacheName, topology, availabilityMode, isTotalOrder(), isDistributed());
               clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, topology, isTotalOrder(), isDistributed());
               return new CacheStatusResponse(null, currentTopology, stableTopology, availabilityMode);
            }
         } else {
            if (isFirstMember) {
               // This node was the first to join. We need to install the initial CH
               CacheTopology initialTopology = createInitialCacheTopology();

               // Change our status
               status = ComponentStatus.RUNNING;

               // Don't need to broadcast the initial CH update, just return the cache topology to the joiner
               // But we do need to broadcast the initial topology as the stable topology
               clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, initialTopology, isTotalOrder(), isDistributed());
            }
         }
      }

      CacheTopology topologyBeforeRebalance = getCurrentTopology();
      // Only trigger availability strategy if we have a topology installed
      if (topologyBeforeRebalance != null)
         availabilityStrategy.onJoin(this, joiner);

      return new CacheStatusResponse(null, topologyBeforeRebalance, stableTopology, availabilityMode);
   }

   @GuardedBy("this")
   protected CacheTopology restoreCacheTopology(ScopedPersistentState state) {
      if (trace) log.tracef("Attempting to restore CH for cache %s", cacheName);

      ConsistentHash persistedCH = joinInfo.getConsistentHashFactory().fromPersistentState(state).remapAddresses(persistentUUIDManager.persistentUUIDToAddress());
      if (persistedCH == null || !getExpectedMembers().containsAll(persistedCH.getMembers())) {
         if (trace) log.tracef("Could not restore CH for cache %s, one or more addresses are missing", cacheName);
         return null;
      }

      if (getExpectedMembers().size() > persistedCH.getMembers().size()) {
         List<Address> extraneousMembers = new ArrayList<>(getExpectedMembers());
         extraneousMembers.removeAll(persistedCH.getMembers());
         throw log.extraneousMembersJoinRestoredCache(extraneousMembers, cacheName);
      }
      CacheTopology initialTopology = new CacheTopology(initialTopologyId, INITIAL_REBALANCE_ID, persistedCH, null,
            CacheTopology.Phase.NO_REBALANCE, persistedCH.getMembers(), persistentUUIDManager.mapAddresses(persistedCH.getMembers()));
      setCurrentTopology(initialTopology);
      setStableTopology(initialTopology);
      rebalancingEnabled = true;
      availabilityMode = AvailabilityMode.AVAILABLE;
      return initialTopology;
   }

   @GuardedBy("this")
   protected CacheTopology createInitialCacheTopology() {
      log.tracef("Initializing status for cache %s", cacheName);
      List<Address> initialMembers = getExpectedMembers();
      ConsistentHash initialCH = joinInfo.getConsistentHashFactory().create(
            joinInfo.getHashFunction(), joinInfo.getNumOwners(), joinInfo.getNumSegments(),
            initialMembers, getCapacityFactors());
      CacheTopology initialTopology = new CacheTopology(initialTopologyId, INITIAL_REBALANCE_ID, initialCH, null,
            CacheTopology.Phase.NO_REBALANCE, initialMembers, persistentUUIDManager.mapAddresses(initialMembers));
      setCurrentTopology(initialTopology);
      setStableTopology(initialTopology);
      return initialTopology;
   }

   public synchronized boolean doLeave(Address leaver) throws Exception {
      if (currentTopology == null)
         return false;

      boolean actualLeaver = removeMember(leaver);
      if (!actualLeaver)
         return false;

      availabilityStrategy.onGracefulLeave(this, leaver);

      updateMembers();

      return expectedMembers.isEmpty();
   }

   public synchronized void startQueuedRebalance() {
      // We cannot start rebalance until queued CR is complete
      if (conflictResolution != null) {
         log.tracef("Postponing rebalance for cache %s as conflict resolution is in progress", cacheName);
         return;
      }

      if (queuedRebalanceMembers == null) {
         // We don't have a queued rebalance. We may need to broadcast a stable topology update
         if (stableTopology == null || stableTopology.getTopologyId() < currentTopology.getTopologyId()) {
            stableTopology = currentTopology;
            log.updatingStableTopology(cacheName, stableTopology);
            clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, stableTopology, isTotalOrder(), isDistributed());
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

      ConsistentHashFactory chFactory = getJoinInfo().getConsistentHashFactory();
      // This update will only add the joiners to the CH, we have already checked that we don't have leavers
      ConsistentHash updatedMembersCH = chFactory.updateMembers(currentCH, newMembers, getCapacityFactors());
      ConsistentHash balancedCH = chFactory.rebalance(updatedMembersCH);

      boolean updateTopology = false;
      boolean rebalance = false;
      boolean updateStableTopology = false;
      if (rebalanceType == RebalanceType.NONE) {
         updateTopology = true;
      } else if (balancedCH.equals(currentCH)) {
         log.tracef("The balanced CH is the same as the current CH, not rebalancing");
         updateTopology = cacheTopology.getPendingCH() != null;
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
         clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, getAvailabilityMode(), isTotalOrder(), isDistributed());
      } else if (rebalance) {
         CacheTopology.Phase newPhase;
         switch (rebalanceType) {
            case FOUR_PHASE:
               newPhase = CacheTopology.Phase.READ_OLD_WRITE_ALL;
               break;
            case TWO_PHASE:
               newPhase = CacheTopology.Phase.TRANSITORY;
               break;
            default:
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
         clusterTopologyManager.broadcastRebalanceStart(cacheName, newTopology, isTotalOrder(), isDistributed());
      } else if (updateStableTopology) {
         stableTopology = currentTopology;
         clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, stableTopology, isTotalOrder(), isDistributed());
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

   public synchronized void setRebalanceEnabled(boolean enabled) {
      rebalancingEnabled = enabled;
      if (rebalancingEnabled) {
         log.debugf("Rebalancing is now enabled for cache %s", cacheName);
         startQueuedRebalance();
      } else {
         log.debugf("Rebalancing is now disabled for cache %s", cacheName);
      }
   }

   public void forceRebalance() {
      queueRebalance(getCurrentTopology().getMembers());
      startQueuedRebalance();
   }

   public synchronized void forceAvailabilityMode(AvailabilityMode newAvailabilityMode) {
      if (currentTopology != null && newAvailabilityMode != availabilityMode) {
         availabilityStrategy.onManualAvailabilityChange(this, newAvailabilityMode);
      }
   }

   public synchronized void shutdownCache() throws Exception {
      if (status == ComponentStatus.RUNNING) {
         status = ComponentStatus.STOPPING;
         clusterTopologyManager.setRebalancingEnabled(cacheName, false);
         clusterTopologyManager.broadcastShutdownCache(cacheName, this.getCurrentTopology(), this.isTotalOrder(), this.isDistributed());
         status = ComponentStatus.TERMINATED;
      }
   }

   public synchronized void setInitialTopologyId(int initialTopologyId) {
      this.initialTopologyId = initialTopologyId;
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
      ConsistentHashFactory chf = getJoinInfo().getConsistentHashFactory();
      ConsistentHash unionHash = distinctHashes.stream().reduce(preferredHash, chf::union);
      unionHash = chf.union(unionHash, chf.rebalance(unionHash));
      return chf.updateMembers(unionHash, actualMembers, capacityFactors);
   }

   @Override
   public synchronized void queueConflictResolution(final CacheTopology conflictTopology, final Set<Address> preferredNodes) {
      if (resolveConflictsOnMerge()) {
         conflictResolution = new ConflictResolution();
         CompletableFuture<Void> resolutionFuture = conflictResolution.queue(conflictTopology, preferredNodes);
         resolutionFuture.thenRun(this::completeConflictResolution);
      }
   }

   private synchronized void completeConflictResolution() {
      if (trace) log.tracef("Cache %s conflict resolution future complete", cacheName);
      // CR is only queued for PreferConsistencyStrategy when a merge it is determined that the newAvailabilityMode will be AVAILABLE
      // therefore if this method is called we know that the partition must be set to AVAILABLE
      availabilityMode = AvailabilityMode.AVAILABLE;

      // Install a NO_REBALANCE topology with pendingCh == null to signal conflict resolution has finished
      CacheTopology conflictTopology = conflictResolution.topology;
      CacheTopology newTopology = new CacheTopology(conflictTopology.getTopologyId() + 1, conflictTopology.getRebalanceId(), conflictTopology.getCurrentCH(),
            null, CacheTopology.Phase.NO_REBALANCE, conflictTopology.getActualMembers(), persistentUUIDManager.mapAddresses(conflictTopology.getActualMembers()));

      conflictResolution = null;
      setCurrentTopology(newTopology);
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode, isTotalOrder(), isDistributed());

      List<Address> actualMembers = conflictTopology.getActualMembers();
      List<Address> newMembers = getExpectedMembers();
      updateAvailabilityMode(actualMembers, availabilityMode, false);

      // Update the topology to remove leavers - in case there is a rebalance in progress, or rebalancing is disabled
      updateCurrentTopology(newMembers);
      // Then queue a rebalance to include the joiners as well
      queueRebalance(newMembers);
   }

   @Override
   public synchronized boolean restartConflictResolution(List<Address> members) {
      // If conflictResolution is null then no CR in progress
      if (!resolveConflictsOnMerge() || conflictResolution == null )
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
         if (trace) log.tracef("Cache %s not restarting conflict resolution, existing conflict topology contains all members (%s)", cacheName, members);
         return false;
      }

      CacheTopology conflictTopology = conflictResolution.topology;
      ConsistentHashFactory chf = getJoinInfo().getConsistentHashFactory();
      ConsistentHash newHash = chf.updateMembers(conflictTopology.getCurrentCH(), members, capacityFactors);

      conflictTopology = new CacheTopology(currentTopology.getTopologyId() + 1, currentTopology.getRebalanceId(),
            newHash, null, CacheTopology.Phase.CONFLICT_RESOLUTION, members, persistentUUIDManager.mapAddresses(members));
      currentTopology = conflictTopology;

      log.debugf("Cache %s restarting conflict resolution with topology %s", cacheName, currentTopology);
      clusterTopologyManager.broadcastTopologyUpdate(cacheName, conflictTopology, availabilityMode, isTotalOrder(), isDistributed());

      queueConflictResolution(conflictTopology, conflictResolution.preferredNodes);
      return true;
   }

   private synchronized void cancelConflictResolutionPhase(CacheTopology resolutionTopology) {
      if (conflictResolution != null) {
         // If the passed topology is not the same as the passed topologyId, then we know that a new
         // ConflictResolution attempt has been queued and therefore we should let this proceed.
         // This check is necessary as it is possible that the call to this method is blocked by
         // a concurrent operation on ClusterCacheStatus that may invalidate the cancel request
         if (conflictResolution.topology.getTopologyId() > resolutionTopology.getTopologyId())
            return;
         completeConflictResolution();
      }
   }

   private class ConflictResolution {
      final CompletableFuture<Void> future = new CompletableFuture<>();
      final AtomicBoolean cancelledLocally = new AtomicBoolean();
      final InternalConflictManager<?, ?> manager;
      volatile CacheTopology topology;
      volatile Set<Address> preferredNodes;

      ConflictResolution() {
         AdvancedCache<?, ?> cache = cacheManager.getCache(cacheName).getAdvancedCache();
         this.manager = (InternalConflictManager) ConflictManagerFactory.get(cache);
      }

      synchronized CompletableFuture<Void> queue(CacheTopology topology, Set<Address> preferredNodes) {
         this.topology = topology;
         this.preferredNodes = preferredNodes;

         log.debugf("Cache %s queueing conflict resolution with members %s", cacheName, topology.getMembers());

         LogFactory.CLUSTER.startingConflictResolution(cacheName, currentTopology);
         eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.conflictResolutionStarting(
               currentTopology.getMembers(), currentTopology.getTopologyId()));

         AdvancedCache<?, ?> cache = cacheManager.getCache(cacheName).getAdvancedCache();
         DefaultConflictManager<?, ?> conflictManager = (DefaultConflictManager) ConflictManagerFactory.get(cache);
         manager.resolveConflicts(topology, preferredNodes).whenComplete((Void, t) -> {
            if (t == null) {
               LogFactory.CLUSTER.finishedConflictResolution(cacheName, currentTopology);
               eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.conflictResolutionFinished(
                  topology.getMembers(), topology.getTopologyId()));
               future.complete(null);
            } else {
               // TODO Add log event for cancel/restart
               if (cancelledLocally.get()) {
                  // We have explicitly cancelled the request, therefore return and do nothing
                  LogFactory.CLUSTER.cancelledConflictResolution(cacheName, topology);
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
                  LogFactory.CLUSTER.failedConflictResolution(cacheName, topology, rootCause);
                  eventLogger.info(EventLogCategory.CLUSTER, MESSAGES.conflictResolutionFailed(
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
      }

      synchronized void cancelCurrentAttempt() {
         cancelledLocally.set(true);
         manager.cancelConflictResolution();
      }

      synchronized boolean restartRequired(List<Address> newMembers) {
         assert newMembers != null;
         return !newMembers.equals(topology.getMembers());
      }
   }
}
