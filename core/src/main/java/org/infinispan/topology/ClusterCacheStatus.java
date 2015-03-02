package org.infinispan.topology;

import net.jcip.annotations.GuardedBy;
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

import static java.lang.String.format;

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
   private volatile ConsistentHash newConsistentHash;

   private volatile TopologyConfirmationCollector confirmationCollector;
   private TopologyState state = TopologyState.NONE;

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

   private void setCurrentTopology(CacheTopology newTopology) {
      synchronized (this) {
         this.currentTopology = newTopology;

         // update the joiners list
         if (newTopology != null) {
            joiners = immutableRemoveAll(expectedMembers, newTopology.getWriteConsistentHash().getMembers());
         }
         if (trace) log.tracef("Cache %s topology updated: %s, members = %s, joiners = %s",
                               cacheName, currentTopology, expectedMembers, joiners);
         if (newTopology != null) {
            newTopology.logRoutingTableInformation();
         }
      }
   }

   @Override
   public CacheTopology getStableTopology() {
      return stableTopology;
   }

   private void setStableTopology(CacheTopology newTopology) {
      synchronized (this) {
         this.stableTopology = newTopology;
         if (trace) log.tracef("Cache %s stable topology updated: members = %s, joiners = %s, topology = %s",
                               cacheName, expectedMembers, joiners, newTopology);
      }
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
            if (cancelRebalance) {
               if (isRebalanceInProgress()) {
                  resetCollector();
               }
            }
            CacheTopology newTopology = new CacheTopology(currentTopology.getTopologyId() + 1,
                                                          currentTopology.getRebalanceId(), currentTopology.getReadConsistentHash(), currentTopology.getWriteConsistentHash(), actualMembers);
            setCurrentTopology(newTopology);
            clusterTopologyManager.broadcastConsistentHashUpdate(cacheName, newTopology, newConsistentHash, newAvailabilityMode,
                                                                 state, isTotalOrder(), isDistributed());
         }
      }
   }

   @Override
   public void updateTopologiesAfterMerge(CacheTopology currentTopology, CacheTopology stableTopology, AvailabilityMode availabilityMode) {
      // This method must be called while holding the lock anyway
      synchronized (this) {
         log.debugf("Updating topologies after merge for cache %s, current topology = %s, stable topology = %s, availability mode = %s",
                    cacheName, currentTopology, stableTopology, availabilityMode);
         this.currentTopology = currentTopology;
         this.stableTopology = stableTopology;
         this.availabilityMode = availabilityMode;

         if (currentTopology != null) {
            clusterTopologyManager.broadcastConsistentHashUpdate(cacheName, currentTopology, newConsistentHash,
                                                                 availabilityMode, state, isTotalOrder(), isDistributed());
         }
         if (stableTopology != null) {
            clusterTopologyManager.broadcastStableTopologyUpdate(cacheName, stableTopology, isTotalOrder(), isDistributed());
         }
      }
   }

   public boolean isRebalanceInProgress() {
      return confirmationCollector != null;
   }

   public void doConfirmRebalance(Address member, int receivedTopologyId) {
      synchronized (this) {
         confirm(member, receivedTopologyId, TopologyState.REBALANCE);
      }
   }

   public void doConfirmReadCH(Address member, int receivedTopologyId) {
      synchronized (this) {
         confirm(member, receivedTopologyId, TopologyState.READ_CH_UPDATE);
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
            updateRebalanceMembers();
         }
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
         ConsistentHash readCH = currentTopology.getReadConsistentHash();
         ConsistentHash writeCH = currentTopology.getWriteConsistentHash();
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
               resetCollector();
            }
            // TODO Remove the cache from the cache status map in ClusterTopologyManagerImpl instead
            return;
         }

         // ReplicatedConsistentHashFactory allocates segments to all its members, so we can't add any members here
         if (pruneInvalidMembers(readCH.getMembers()).isEmpty()) {
            // All the current members left, try to replace them with the joiners
            createInitialCacheTopology();
            return;
         }

         CacheTopology newTopology;
         if (readCH.equals(writeCH)) {
            List<Address> actualMembers = pruneInvalidMembers(readCH.getMembers());
            ConsistentHash newReadCH = consistentHashFactory.updateMembers(readCH, actualMembers, getCapacityFactors());
            newTopology = new CacheTopology(topologyId + 1, rebalanceId, newReadCH, newReadCH, actualMembers);
         } else {
            List<Address> actualMembers = pruneInvalidMembers(writeCH.getMembers());
            ConsistentHash newReadCH = consistentHashFactory.updateMembers(readCH, pruneInvalidMembers(readCH.getMembers()), getCapacityFactors());
            ConsistentHash newWriteCH = consistentHashFactory.updateMembers(writeCH, actualMembers, getCapacityFactors());

            newTopology = new CacheTopology(topologyId + 1, rebalanceId, newReadCH, newWriteCH, actualMembers);
         }
         setCurrentTopology(newTopology);

         if (newConsistentHash != null) {
            newConsistentHash = consistentHashFactory.updateMembers(newConsistentHash, pruneInvalidMembers(newConsistentHash.getMembers()), getCapacityFactors());
         }

         clusterTopologyManager.broadcastConsistentHashUpdate(cacheName, newTopology, newConsistentHash, availabilityMode,
                                                              state, isTotalOrder(), isDistributed());
      }
   }

   @Override
   public String toString() {
      return "ClusterCacheStatus{" +
            "cacheName='" + cacheName + '\'' +
            ", members=" + expectedMembers +
            ", joiners=" + joiners +
            ", currentTopology=" + currentTopology +
            ", confirmationCollector=" + confirmationCollector +
            '}';
   }

   public void doMergePartitions(Map<Address, CacheStatusResponse> statusResponses) throws Exception {
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
            createInitialCacheTopology();
         }
         topologyBeforeRebalance = getCurrentTopology();

         availabilityStrategy.onJoin(this, joiner);
      }

      return new CacheStatusResponse(null, topologyBeforeRebalance, stableTopology, availabilityMode);
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

         updateRebalanceMembers();
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

         List<Address> newMembers = new ArrayList<>(queuedRebalanceMembers);
         queuedRebalanceMembers = null;
         log.tracef("Rebalancing consistent hash for cache %s, members are %s", cacheName, newMembers);

         if (cacheTopology == null) {
            createInitialCacheTopology();
            return;
         }

         int newTopologyId = cacheTopology.getTopologyId() + 1;
         int newRebalanceId = cacheTopology.getRebalanceId() + 1;
         ConsistentHash readCH = cacheTopology.getReadConsistentHash();
         if (readCH == null) {
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

         ConsistentHashFactory<ConsistentHash> chFactory = getJoinInfo().getConsistentHashFactory();
         // This update will only add the joiners to the CH, we have already checked that we don't have leavers
         ConsistentHash updatedMembersCH = chFactory.updateMembers(readCH, newMembers, getCapacityFactors());
         newConsistentHash = chFactory.rebalance(updatedMembersCH);
         if (newConsistentHash.equals(readCH) && cacheTopology.isStable()) {
            newConsistentHash = null;
            log.tracef("The balanced CH is the same as the current CH, not rebalancing");
            return;
         }

         ConsistentHash writeCH = chFactory.union(readCH, newConsistentHash);
         CacheTopology newTopology = new CacheTopology(newTopologyId, newRebalanceId, readCH, writeCH, writeCH.getMembers());
         log.tracef("Updating cache %s topology for rebalance: %s", cacheName, newTopology);
         setCurrentTopology(newTopology);
         initRebalanceConfirmationCollector(newTopology);
      }

      clusterTopologyManager.broadcastRebalanceStart(cacheName, this.getCurrentTopology(), newConsistentHash,
                                                     this.isTotalOrder(), this.isDistributed());
   }

   public void forceRebalance() {
      queueRebalance(getCurrentTopology().getMembers());
      startQueuedRebalance();
   }

   public void forceAvailabilityMode(AvailabilityMode newAvailabilityMode) {
      availabilityStrategy.onManualAvailabilityChange(this, newAvailabilityMode);
   }

   protected CacheTopology createInitialCacheTopology() {
      List<Address> initialMembers = getExpectedMembers();
      CacheTopology initialTopology = null;
      if (!initialMembers.isEmpty()) {
         ConsistentHash initialCH = joinInfo.getConsistentHashFactory().create(
               joinInfo.getHashFunction(), joinInfo.getNumOwners(), joinInfo.getNumSegments(),
               initialMembers, getCapacityFactors());
         initialTopology = new CacheTopology(0, 0, initialCH, initialCH, initialMembers);
      }
      setCurrentTopology(initialTopology);
      setStableTopology(initialTopology);
      return initialTopology;
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

         HashMap<Address, Float> newCapacityFactors = new HashMap<>(capacityFactors);
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
         HashMap<Address, Float> newCapacityFactors = new HashMap<>(capacityFactors);
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

   private boolean needConsistentHashUpdate() {
      // The list of current members is always included in the list of pending members,
      // so we only need to check one list.
      // Also returns false if both CHs are null
      return !expectedMembers.equals(currentTopology.getMembers());
   }

   private List<Address> pruneInvalidMembers(List<Address> possibleMembers) {
      return immutableRetainAll(possibleMembers, expectedMembers);
   }

   private void confirm(Address member, int receivedTopologyId, TopologyState type) {
      if (confirmationCollector == null || type != state) {
         throw new CacheException(
               format("Received invalid topology confirmation from %s for cache %s. Collector exists? %b. Collector type %s (received %s)",
                      member, cacheName, confirmationCollector != null, state, type));
      }
      confirmationCollector.confirm(member, receivedTopologyId);
   }

   /**
    * Should be called after the members list was updated in any other way ({@link #removeMember(Address)}, {@link
    * #retainMembers} etc.)
    */
   private void updateRebalanceMembers() {
      synchronized (this) {
         // We rely on the AvailabilityStrategy updating the current topology beforehand.
         if (confirmationCollector != null) {
            confirmationCollector.updateMembers(currentTopology.getMembers());
         }
      }
   }

   private void triggerReadConsistentHashUpdate() {
      synchronized (this) {

         CacheTopology currentTopology = getCurrentTopology();
         if (currentTopology == null) {
            log.tracef("Rebalance finished because there are no more members in cache %s", cacheName);
            return;
         }

         final int currentTopologyId = currentTopology.getTopologyId();
         log.debugf("Finished cluster-wide rebalance for cache %s, topology id = %d", cacheName, currentTopologyId);
         final int newTopologyId = currentTopologyId + 1;

         final ConsistentHash oldConsistentHash = currentTopology.getReadConsistentHash();
         final ConsistentHash newReadCH = newConsistentHash;
         ConsistentHash newWriteCH = getJoinInfo().getConsistentHashFactory().union(newConsistentHash, oldConsistentHash);
         this.newConsistentHash = null;

         //update read ch
         CacheTopology newTopology = new CacheTopology(newTopologyId, getCurrentTopology().getRebalanceId(),
                                                       newReadCH, newWriteCH, newWriteCH.getMembers());
         setCurrentTopology(newTopology);
         initReadCHConfirmationCollector(newTopology);

         clusterTopologyManager.broadcastReadConsistentHashUpdate(cacheName, newTopology, availabilityMode,
                                                                  isTotalOrder(), isDistributed());

      }
   }

   private void triggerWriteConsistentHashUpdate() {
      synchronized (this) {
         removeReadCHConfirmationCollector();

         CacheTopology currentTopology = getCurrentTopology();
         if (currentTopology == null) {
            log.tracef("Rebalance finished because there are no more members in cache %s", cacheName);
            return;
         }

         final ConsistentHash consistentHash = currentTopology.getReadConsistentHash();

         //update write ch
         CacheTopology newTopology = new CacheTopology(currentTopology.getTopologyId() + 1,
                                                       getCurrentTopology().getRebalanceId(),
                                                       consistentHash, consistentHash, consistentHash.getMembers());

         clusterTopologyManager.broadcastWriteConsistentHashUpdate(cacheName, newTopology, availabilityMode,
                                                                   isTotalOrder(), isDistributed());

         setCurrentTopology(newTopology);

         availabilityStrategy.onRebalanceEnd(this);
         startQueuedRebalance();
      }
   }

   /**
    * It creates a new {@link org.infinispan.topology.TopologyConfirmationCollector} for {@code confirmationCollector}.
    * <p/>
    * If {@code confirmationCollector} exists, this method is no-op.
    * <p/>
    * Also, it isn't synchronized and then it should be wrapped.
    */
   @GuardedBy("this")
   private void initRebalanceConfirmationCollector(CacheTopology newTopology) {
      if (confirmationCollector != null || state != TopologyState.NONE) {
         return;
      }

      state = TopologyState.REBALANCE;
      confirmationCollector = new TopologyConfirmationCollector(cacheName, newTopology.getTopologyId(),
                                                                newTopology.getMembers(), new Runnable() {
         @Override
         public void run() {
            triggerReadConsistentHashUpdate();
         }
      });
   }

   /**
    * It creates a new {@link org.infinispan.topology.TopologyConfirmationCollector} for {@code
    * readCHConfirmationCollector}.
    * <p/>
    * If {@code readCHConfirmationCollector} exists, this method is no-op.
    * <p/>
    * Also, it isn't synchronized and then it should be wrapped.
    */
   @GuardedBy("this")
   private void initReadCHConfirmationCollector(final CacheTopology newTopology) {
      if (state != TopologyState.REBALANCE) {
         return;
      }

      state = TopologyState.READ_CH_UPDATE;
      confirmationCollector = new TopologyConfirmationCollector(cacheName, newTopology.getTopologyId(),
                                                                newTopology.getMembers(), new Runnable() {
         @Override
         public void run() {
            triggerWriteConsistentHashUpdate();
         }
      });
   }

   /**
    * It removes the current {@code readCHConfirmationCollector}.
    *
    * @throws java.lang.IllegalStateException if no {@code confirmationCollector} exists.
    */
   @GuardedBy("this")
   private void removeReadCHConfirmationCollector() {
      if (confirmationCollector == null || state != TopologyState.READ_CH_UPDATE) {
         throw new IllegalStateException("Can't end rebalance, there is no rebalance in progress");
      }
      state = TopologyState.NONE;
      confirmationCollector = null;
   }

   @GuardedBy("this")
   private void resetCollector() {
      state = TopologyState.NONE;
      confirmationCollector = null;
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

   private boolean isRebalanceEnabled() {
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
}
