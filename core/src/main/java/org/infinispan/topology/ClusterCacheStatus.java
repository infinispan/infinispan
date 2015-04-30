package org.infinispan.topology;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.AvailabilityStrategy;
import org.infinispan.partitionhandling.impl.AvailabilityStrategyContext;
import org.infinispan.registry.impl.ClusterRegistryImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.util.logging.LogFactory.CLUSTER;

/**
* Keeps track of a cache's status: members, current/pending consistent hashes, and rebalance status
*
* @author Dan Berindei
* @since 5.2
*/
public class ClusterCacheStatus implements AvailabilityStrategyContext {
   private static final Log log = LogFactory.getLog(ClusterCacheStatus.class);
   private static boolean trace = log.isTraceEnabled();

   private final String cacheName;
   private final AvailabilityStrategy availabilityStrategy;
   private final ClusterTopologyManager clusterTopologyManager;
   private Transport transport;

   // Minimal cache clustering configuration
   private volatile CacheJoinInfo joinInfo;
   // Cache members, some of which may not have received state yet
   private volatile List<Address> expectedMembers;
   // Capacity factors for all the members
   private volatile Map<Address, Float> capacityFactors;
   // Cache members that have not yet received state. Always included in the members list.
   private volatile List<Address> joiners;
   // Cache topology. Its consistent hashes contain only members that did receive/are receiving state
   // The members of both consistent hashes must be included in the members list.
   private volatile CacheTopology currentTopology;
   private volatile CacheTopology stableTopology;
   private volatile AvailabilityMode availabilityMode = AvailabilityMode.AVAILABLE;
   private volatile List<Address> queuedRebalanceMembers;

   private volatile RebalanceConfirmationCollector rebalanceConfirmationCollector;

   public ClusterCacheStatus(String cacheName, AvailabilityStrategy availabilityStrategy,
         ClusterTopologyManager clusterTopologyManager, Transport transport) {
      this.cacheName = cacheName;
      this.availabilityStrategy = availabilityStrategy;
      this.clusterTopologyManager = clusterTopologyManager;
      this.transport = transport;

      this.currentTopology = null;
      this.stableTopology = null;
      this.expectedMembers = InfinispanCollections.emptyList();
      this.capacityFactors = InfinispanCollections.emptyMap();
      this.joiners = InfinispanCollections.emptyList();
      if (trace) log.tracef("Cache %s initialized", cacheName);
   }

   public CacheJoinInfo getJoinInfo() {
      return joinInfo;
   }

   @Override
   public List<Address> getExpectedMembers() {
      return expectedMembers;
   }

   @Override
   public void queueRebalance(List<Address> newMembers) {
      synchronized (this) {
         if (newMembers != null && !newMembers.isEmpty()) {
            log.debugf("Queueing rebalance for cache %s with members %s", cacheName, newMembers);
            queuedRebalanceMembers = newMembers;
            startQueuedRebalance();
         }
      }
   }

   public boolean isTotalOrder() {
      return joinInfo.isTotalOrder();
   }

   public boolean isDistributed() {
      return joinInfo.isDistributed();
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
   public void updateAvailabilityMode(List<Address> actualMembers, AvailabilityMode newAvailabilityMode,
         boolean cancelRebalance) {
      synchronized (this) {
         boolean modeChanged = setAvailabilityMode(newAvailabilityMode);

         if (modeChanged || !actualMembers.equals(currentTopology.getActualMembers())) {
            log.debugf("Updating availability for cache %s to %s", cacheName, newAvailabilityMode);
            ConsistentHash newPendingCH = currentTopology.getPendingCH();
            if (cancelRebalance) {
               newPendingCH = null;
               if (isRebalanceInProgress()) {
                  removeRebalanceConfirmationCollector();
               }
            }
            CacheTopology newTopology = new CacheTopology(currentTopology.getTopologyId() + 1,
                  currentTopology.getRebalanceId(), currentTopology.getCurrentCH(), newPendingCH, actualMembers);
            setCurrentTopology(newTopology);
            clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, newAvailabilityMode,
                  isTotalOrder(), isDistributed());
         }
      }
   }

   @Override
   public void updateTopologiesAfterMerge(CacheTopology currentTopology, CacheTopology stableTopology, AvailabilityMode availabilityMode) {
      // This method must be called while holding the lock anyway
      synchronized (this) {
         log.debugf("Updating topologies after merge for cache %s, current topology = %s, stable topology = %s, " +
                     "availability mode = %s",
               cacheName, currentTopology, stableTopology, availabilityMode);
         this.currentTopology = currentTopology;
         this.stableTopology = stableTopology;
         this.availabilityMode = availabilityMode;

         if (currentTopology != null) {
            clusterTopologyManager.broadcastTopologyUpdate(cacheName, currentTopology, availabilityMode, isTotalOrder(), isDistributed());
         }
         if (stableTopology != null) {
            clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, stableTopology, isTotalOrder(),
                  isDistributed());
         }
      }
   }

   /**
    * @return {@code true} if the joiner was not already a member, {@code false} otherwise
    */
   private boolean addMember(Address joiner, CacheJoinInfo joinInfo) {
      synchronized (this) {
         if (expectedMembers.contains(joiner)) {
            return false;
         }

         if (this.joinInfo == null) {
            this.joinInfo = joinInfo;
         }

         HashMap<Address, Float> newCapacityFactors = new HashMap<Address, Float>(capacityFactors);
         newCapacityFactors.put(joiner, joinInfo.getCapacityFactor());
         capacityFactors = Immutables.immutableMapWrap(newCapacityFactors);
         expectedMembers = immutableAdd(expectedMembers, joiner);
         joiners = immutableAdd(joiners, joiner);
         if (trace) log.tracef("Added joiner %s to cache %s: members = %s, joiners = %s", joiner, cacheName,
               expectedMembers, joiners);
         return true;
      }
   }

   /**
    * @return {@code true} if the leaver was a member, {@code false} otherwise
    */
   private boolean removeMember(Address leaver) {
      synchronized (this) {
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
   }

   /**
    * @return {@code true} if the members list has changed, {@code false} otherwise
    */
   private boolean retainMembers(List<Address> newClusterMembers) {
      synchronized (this) {
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
   }

   private void setCurrentTopology(CacheTopology newTopology) {
      synchronized (this) {
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
   }

   private void setStableTopology(CacheTopology newTopology) {
      synchronized (this) {
         this.stableTopology = newTopology;
         if (trace) log.tracef("Cache %s stable topology updated: members = %s, joiners = %s, topology = %s",
               cacheName, expectedMembers, joiners, newTopology);
      }
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

   /**
    * @return {@code true} if a rebalance was started, {@code false} if a rebalance was already in progress
    */
   private boolean initRebalanceConfirmationCollector(CacheTopology newTopology) {
      synchronized (this) {
         if (rebalanceConfirmationCollector != null)
            return false;

         rebalanceConfirmationCollector = new RebalanceConfirmationCollector(cacheName, newTopology.getTopologyId(),
               newTopology.getMembers());
         return true;
      }
   }

   public void doConfirmRebalance(Address member, int receivedTopologyId) throws Exception {
      synchronized (this) {
         if (rebalanceConfirmationCollector == null) {
            throw new CacheException(String.format("Received invalid rebalance confirmation from %s " +
                  "for cache %s, we don't have a rebalance in progress", member, cacheName));
         }

         boolean rebalanceCompleted = rebalanceConfirmationCollector.confirmRebalance(member, receivedTopologyId);
         if (rebalanceCompleted) {
            endRebalance();
         }
      }
   }

   /**
    * Should be called after the members list was updated in any other way ({@link #removeMember(Address)},
    * {@link #retainMembers} etc.)
    *
    * @return {@code true} if the rebalance was confirmed with this update, {@code false} if more confirmations
    *    are needed or if the rebalance was already confirmed in another way (e.g. the last member confirmed)
    */
   private boolean updateRebalanceMembers() {
      synchronized (this) {
         if (rebalanceConfirmationCollector == null)
            return false;

         // We rely on the AvailabilityStrategy updating the current topology beforehand.
         return rebalanceConfirmationCollector.updateMembers(currentTopology.getMembers());
      }
   }

   public void doHandleClusterView() throws Exception {
      synchronized (this) {
         // TODO Clean up ClusterCacheStatus instances once they no longer have any members
         if (currentTopology == null)
            return;

         List<Address> newClusterMembers = transport.getMembers();
         boolean cacheMembersModified = retainMembers(newClusterMembers);
         availabilityStrategy.onClusterViewChange(this, newClusterMembers);

         if (cacheMembersModified) {
            boolean rebalanceCompleted = updateRebalanceMembers();
            if (rebalanceCompleted) {
               endRebalance();
            }
         }
      }
   }

   private void endRebalance() {
      synchronized (this) {
         removeRebalanceConfirmationCollector();

         CacheTopology currentTopology = getCurrentTopology();
         if (currentTopology == null) {
            log.tracef("Rebalance finished because there are no more members in cache %s", cacheName);
            return;
         }

         int currentTopologyId = currentTopology.getTopologyId();
         CLUSTER.clusterWideRebalanceCompleted(cacheName, currentTopologyId);
         int newTopologyId = currentTopologyId + 1;
         ConsistentHash newCurrentCH = currentTopology.getPendingCH();
         CacheTopology newTopology = new CacheTopology(newTopologyId, currentTopology.getRebalanceId(),
               newCurrentCH, null, newCurrentCH.getMembers());
         setCurrentTopology(newTopology);

         clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode,
               isTotalOrder(), isDistributed());

         availabilityStrategy.onRebalanceEnd(this);
         startQueuedRebalance();
      }
   }

   private void removeRebalanceConfirmationCollector() {
      synchronized (this) {
         if (rebalanceConfirmationCollector == null) {
            throw new IllegalStateException("Can't end rebalance, there is no rebalance in progress");
         }
         rebalanceConfirmationCollector = null;
      }
   }

   @Override
   public void updateCurrentTopology(List<Address> newMembers) {
      synchronized (this) {
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
            if (isRebalanceInProgress()) {
               removeRebalanceConfirmationCollector();
            }
            // TODO Remove the cache from the cache status map in ClusterTopologyManagerImpl instead
            return;
         }


         List<Address> newCurrentMembers = pruneInvalidMembers(currentCH.getMembers());
         ConsistentHash newCurrentCH;
         List<Address> actualMembers;
         ConsistentHash newPendingCH = null;
         if (newCurrentMembers.isEmpty()) {
            // All the current members left, try to replace them with the joiners
            log.tracef("All current members left, re-initializing status for cache %s", cacheName);
            if (isRebalanceInProgress()) {
               removeRebalanceConfirmationCollector();
            }

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
               List<Address> newPendingMembers = pruneInvalidMembers(pendingCH.getMembers());
               newPendingCH = consistentHashFactory.updateMembers(pendingCH, newPendingMembers, getCapacityFactors());
               actualMembers = newPendingMembers;
            }
         }
         CacheTopology newTopology = new CacheTopology(topologyId + 1, rebalanceId, newCurrentCH, newPendingCH,
               actualMembers);
         setCurrentTopology(newTopology);

         clusterTopologyManager.broadcastTopologyUpdate(cacheName, newTopology, availabilityMode,
               isTotalOrder(), isDistributed());
      }
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

   public void doMergePartitions(Map<Address, CacheStatusResponse> statusResponses,
         List<Address> clusterMembers, boolean isMergeView) throws Exception {
      synchronized (this) {
         if (statusResponses.isEmpty()) {
            throw new IllegalArgumentException("Should have at least one current topology");
         }

         try {
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

            availabilityStrategy.onPartitionMerge(this, statusResponses.values());
         } catch (Exception e) {
            log.failedToRecoverCacheState(cacheName, e);
         }
      }
   }

   private void recoverMembers(Map<Address, CacheJoinInfo> joinInfos,
         Collection<CacheTopology> currentTopologies, Collection<CacheTopology> stableTopologies) {
      expectedMembers = InfinispanCollections.emptyList();

      // Try to preserve the member order at least for the first partition
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

   public CacheStatusResponse doJoin(Address joiner, CacheJoinInfo joinInfo) throws Exception {
      boolean isFirstMember;
      CacheTopology topologyBeforeRebalance;
      synchronized (this) {
         isFirstMember = getCurrentTopology() == null;
         boolean memberJoined = addMember(joiner, joinInfo);
         if (!isFirstMember && !memberJoined) {
            if (trace) log.tracef("Trying to add node %s to cache %s, but it is already a member: " +
                  "members = %s, joiners = %s", joiner, cacheName, expectedMembers, joiners);
            return new CacheStatusResponse(null, currentTopology, stableTopology, availabilityMode);
         }
         if (isFirstMember) {
            // This node was the first to join. We need to install the initial CH
            CacheTopology initialTopology = createInitialCacheTopology();

            // Don't need to broadcast the initial CH update, just return the cache topology to the joiner
            // But we do need to broadcast the initial topology as the stable topology
            clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, initialTopology, isTotalOrder(), isDistributed());
         }
         topologyBeforeRebalance = getCurrentTopology();

         availabilityStrategy.onJoin(this, joiner);
      }

      return new CacheStatusResponse(null, topologyBeforeRebalance, stableTopology, availabilityMode);
   }

   protected CacheTopology createInitialCacheTopology() {
      log.tracef("Initializing status for cache %s", cacheName);
      List<Address> initialMembers = getExpectedMembers();
      ConsistentHash initialCH = joinInfo.getConsistentHashFactory().create(
            joinInfo.getHashFunction(), joinInfo.getNumOwners(), joinInfo.getNumSegments(),
            initialMembers, getCapacityFactors());
      CacheTopology initialTopology = new CacheTopology(0, 0, initialCH, null, initialMembers);
      setCurrentTopology(initialTopology);
      setStableTopology(initialTopology);
      return initialTopology;
   }

   public void doLeave(Address leaver) throws Exception {
      synchronized (this) {
         // TODO Clean up ClusterCacheStatus instances once they no longer have any members
         if (currentTopology == null)
            return;

         boolean actualLeaver = removeMember(leaver);
         if (!actualLeaver)
            return;

         availabilityStrategy.onGracefulLeave(this, leaver);

         boolean rebalanceCompleted = updateRebalanceMembers();
         if (rebalanceCompleted) {
            endRebalance();
         }
      }
   }

   public void startQueuedRebalance() {
      synchronized (this) {
         if (queuedRebalanceMembers == null) {
            // We don't have a queued rebalance. We may need to broadcast a stable topology update
            if (stableTopology == null || stableTopology.getTopologyId() < currentTopology.getTopologyId()) {
               stableTopology = currentTopology;
               log.tracef("Updating stable topology for cache %s: %s", cacheName, stableTopology);
               clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, stableTopology, isTotalOrder(), isDistributed());
            }
            return;
         }

         CacheTopology cacheTopology = getCurrentTopology();
         if (!isRebalanceEnabled() && !cacheName.equals(ClusterRegistryImpl.GLOBAL_REGISTRY_CACHE_NAME)) {
            log.tracef("Postponing rebalance for cache %s, rebalancing is disabled", cacheName);
            return;
         }
         if (isRebalanceInProgress()) {
            log.tracef("Postponing rebalance for cache %s, there's already a rebalance in progress: %s",
                  cacheName, cacheTopology);
            return;
         }

         if (queuedRebalanceMembers.isEmpty()) {
            log.tracef("Ignoring request to rebalance cache %s, it doesn't have any member", cacheName);
            return;
         }

         List<Address> newMembers = new ArrayList<Address>(queuedRebalanceMembers);
         queuedRebalanceMembers = null;
         log.tracef("Rebalancing consistent hash for cache %s, members are %s", cacheName, newMembers);

         if (cacheTopology == null) {
            createInitialCacheTopology();
            return;
         }

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
         if (balancedCH.equals(currentCH)) {
            log.tracef("The balanced CH is the same as the current CH, not rebalancing");
            return;
         }
         CacheTopology newTopology = new CacheTopology(newTopologyId, newRebalanceId, currentCH, balancedCH,
               balancedCH.getMembers());
         log.tracef("Updating cache %s topology for rebalance: %s", cacheName, newTopology);
         setCurrentTopology(newTopology);
         initRebalanceConfirmationCollector(newTopology);
      }

      clusterTopologyManager.broadcastRebalanceStart(cacheName, this.getCurrentTopology(), this.isTotalOrder(), this.isDistributed());
   }

   public boolean isRebalanceEnabled() {
      return clusterTopologyManager.isRebalancingEnabled();
   }

   public void setRebalanceEnabled(boolean enabled) {
      synchronized (this) {
         if (enabled) {
            log.debugf("Rebalancing is now enabled for cache %s", cacheName);
            startQueuedRebalance();
         } else {
            log.debugf("Rebalancing is now disabled for cache %s", cacheName);
         }
      }
   }

   public void forceRebalance() {
      queueRebalance(getCurrentTopology().getMembers());
      startQueuedRebalance();
   }

   public void forceAvailabilityMode(AvailabilityMode newAvailabilityMode) {
      availabilityStrategy.onManualAvailabilityChange(this, newAvailabilityMode);
   }
}
