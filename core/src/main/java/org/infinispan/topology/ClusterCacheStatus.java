package org.infinispan.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.partionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
* Keeps track of a cache's status: members, current/pending consistent hashes, and rebalance status
*
* @author Dan Berindei
* @since 5.2
*/
public class ClusterCacheStatus {
   private static final Log log = LogFactory.getLog(ClusterCacheStatus.class);
   private static boolean trace = log.isTraceEnabled();

   private final String cacheName;
   private final CacheJoinInfo joinInfo;
   private final RebalancePolicy rebalancePolicy;
   private final PartitionHandlingManager partitionHandlingManager;
   // Cache members, some of which may not have received state yet
   private volatile List<Address> members;
   // Capacity factors for all the members
   private volatile Map<Address, Float> capacityFactors;
   // Cache members that have not yet received state. Always included in the members list.
   private volatile List<Address> joiners;
   // Cache topology. Its consistent hashes contain only members that did receive/are receiving state
   // The members of both consistent hashes must be included in the members list.
   private volatile CacheTopology cacheTopology;

   private volatile RebalanceConfirmationCollector rebalanceStatus;

   private Transport transport;

   private GlobalConfiguration globalConfiguration;

   private ExecutorService asyncTransportExecutor;

   private GlobalComponentRegistry gcr;

   public ClusterCacheStatus(String cacheName, CacheJoinInfo joinInfo, Transport transport, GlobalConfiguration globalConfiguration,
                             ExecutorService asyncTransportExecutor, GlobalComponentRegistry gcr, RebalancePolicy rebalancePolicy) {
      this.cacheName = cacheName;
      this.joinInfo = joinInfo;

      this.cacheTopology = new CacheTopology(-1, null, null);
      this.members = InfinispanCollections.emptyList();
      this.capacityFactors = InfinispanCollections.emptyMap();
      this.joiners = InfinispanCollections.emptyList();
      this.transport = transport;
      this.globalConfiguration = globalConfiguration;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.gcr = gcr;
      this.rebalancePolicy = rebalancePolicy;
      if (trace) log.tracef("Cache %s initialized, join info is %s", cacheName, joinInfo);
      partitionHandlingManager = gcr.getNamedComponentRegistry(cacheName).getComponent(PartitionHandlingManager.class);
   }

   public CacheJoinInfo getJoinInfo() {
      return joinInfo;
   }

   public List<Address> getMembers() {
      return members;
   }

   public boolean hasMembers() {
      return !members.isEmpty();
   }

   public List<Address> getJoiners() {
      return joiners;
   }

   public boolean hasJoiners() {
      return !joiners.isEmpty();
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

//   public void setMembers(List<Address> newMembers) {
//      synchronized (this) {
//         members = Immutables.immutableListCopy(newMembers);
//
//         ConsistentHash currentCH = cacheTopology.getCurrentCH();
//         if (currentCH != null) {
//            joiners = immutableRemoveAll(members, currentCH.getMembers());
//         } else {
//            joiners = members;
//         }
//         if (trace) log.tracef("Cache %s members list updated, members = %s, joiners = %s", cacheName,
//               members, joiners);
//      }
//   }
//
   /**
    * @return {@code true} if the joiner was not already a member, {@code false} otherwise
    */
   public boolean addMember(Address joiner, float capacityFactor) {
      synchronized (this) {
         if (members.contains(joiner)) {
            if (trace) log.tracef("Trying to add node %s to cache %s, but it is already a member: " +
                  "members = %s, joiners = %s", joiner, cacheName, members, joiners);
            return false;
         }

         HashMap<Address, Float> newCapacityFactors = new HashMap<Address, Float>(capacityFactors);
         newCapacityFactors.put(joiner, capacityFactor);
         capacityFactors = Immutables.immutableMapWrap(newCapacityFactors);
         members = immutableAdd(members, joiner);
         joiners = immutableAdd(joiners, joiner);
         if (trace) log.tracef("Added joiner %s to cache %s: members = %s, joiners = %s", joiner, cacheName,
               members, joiners);
         return true;
      }
   }

   /**
    * @return {@code true} if the leaver was a member, {@code false} otherwise
    */
   public boolean removeMember(Address leaver) {
      synchronized (this) {
         if (!members.contains(leaver)) {
            if (trace) log.tracef("Trying to remove node %s from cache %s, but it is not a member: " +
                  "members = %s", leaver, cacheName, members);
            return false;
         }

         members = immutableRemove(members, leaver);
         HashMap<Address, Float> newCapacityFactors = new HashMap<Address, Float>(capacityFactors);
         newCapacityFactors.remove(leaver);
         capacityFactors = Immutables.immutableMapWrap(newCapacityFactors);
         joiners = immutableRemove(joiners, leaver);
         if (trace) log.tracef("Removed node %s from cache %s: members = %s, joiners = %s", leaver,
               cacheName, members, joiners);
         return true;
      }
   }

   /**
    * @return {@code true} if the members list has changed, {@code false} otherwise
    */
   public boolean updateClusterMembers(List<Address> newClusterMembers) {
      synchronized (this) {
         if (newClusterMembers.containsAll(members)) {
            if (trace) log.tracef("Cluster members updated for cache %s, no leavers detected: " +
                  "cache members = %s", cacheName, newClusterMembers);
            return false;
         }

         members = immutableRetainAll(members, newClusterMembers);
         joiners = immutableRetainAll(joiners, newClusterMembers);
         if (trace) log.tracef("Cluster members updated for cache %s: members = %s, joiners = %s", cacheName,
               members, joiners);
         return true;
      }
   }

   public CacheTopology getCacheTopology() {
      return cacheTopology;
   }

   public void updateCacheTopology(CacheTopology newTopology) {
      synchronized (this) {
         this.cacheTopology = newTopology;
         if (!members.containsAll(cacheTopology.getMembers())) {
            throw new IllegalStateException(String.format("Trying to set a topology with invalid members " +
                  "for cache %s: members = %s, topology = %s", cacheName, members, cacheTopology));
         }

         // update the joiners list
         if (newTopology.getCurrentCH() != null) {
            joiners = immutableRemoveAll(members, newTopology.getCurrentCH().getMembers());
         }
         if (trace) log.tracef("Cache %s topology updated: members = %s, joiners = %s, topology = %s",
               cacheName, members, joiners, cacheTopology);
      }
   }

   public boolean needConsistentHashUpdate() {
      // The list of current members is always included in the list of pending members,
      // so we only need to check one list.
      // Also returns false if both CHs are null
      return !members.containsAll(cacheTopology.getMembers());
   }

   public List<Address> pruneInvalidMembers(List<Address> possibleMembers) {
      return immutableRetainAll(possibleMembers, members);
   }

   public boolean isRebalanceInProgress() {
      return rebalanceStatus != null;
   }

   /**
    * @return {@code true} if a rebalance was started, {@code false} if a rebalance was already in progress
    */
   public boolean startRebalance(CacheTopology newTopology) {
      synchronized (this) {
         if (rebalanceStatus != null)
            return false;

         rebalanceStatus = new RebalanceConfirmationCollector(cacheName, newTopology.getTopologyId(),
               newTopology.getMembers());
         this.cacheTopology = newTopology;
         return true;
      }
   }

   /**
    * @return {@code true} if this was the last confirmation needed, {@code false} if more confirmations
    *    are needed or if the rebalance was already confirmed in another way (e.g. members list update)
    */
   public boolean confirmRebalanceOnNode(Address member, int receivedTopologyId) {
      synchronized (this) {
         if (rebalanceStatus == null)
            return false;

         return rebalanceStatus.confirmRebalance(member, receivedTopologyId);
      }
   }

   /**
    * Should be called after the members list was updated in any other way ({@link #removeMember(Address)},
    * {@link #updateClusterMembers} etc.)
    *
    * @return {@code true} if the rebalance was confirmed with this update, {@code false} if more confirmations
    *    are needed or if the rebalance was already confirmed in another way (e.g. the last member confirmed)
    */
   public boolean updateRebalanceMembersList() {
      synchronized (this) {
         if (rebalanceStatus == null)
            return false;

         return rebalanceStatus.updateMembers(members);
      }
   }

   public void setRebalanceStatus() {
      synchronized (this) {
         if (rebalanceStatus == null) {
            throw new IllegalStateException("Can't end rebalance, there is no rebalance in progress");
         }
         rebalanceStatus = null;
      }
   }

   public void processMembershipChange(List<Address> newClusterMembers) throws Exception {
      if (partitionHandlingManager == null || partitionHandlingManager.handleViewChange(newClusterMembers, this)) {
         boolean cacheMembersModified = updateClusterMembers(newClusterMembers);
         if (cacheMembersModified) {
            onCacheMembershipChange();
         }
      }
   }

   public boolean onCacheMembershipChange() throws Exception {
      boolean topologyChanged = updateTopologyAfterMembershipChange();
      if (!topologyChanged)
         return true;

      boolean rebalanceCompleted = updateRebalanceMembersList();
      if (rebalanceCompleted) {
         setRebalanceStatus();
      }

      // We need a consistent hash update even when rebalancing did end
      broadcastConsistentHashUpdate();

      rebalancePolicy.updateCacheStatus(cacheName, this);
      return false;
   }

   public void endRebalance() {
      synchronized (this) {
         CacheTopology currentTopology = getCacheTopology();
         int currentTopologyId = currentTopology.getTopologyId();
         log.debugf("Finished cluster-wide rebalance for cache %s, topology id = %d", cacheName, currentTopologyId);
         int newTopologyId = currentTopologyId + 1;
         ConsistentHash newCurrentCH = currentTopology.getPendingCH();
         CacheTopology newTopology = new CacheTopology(newTopologyId, newCurrentCH, null);
         updateCacheTopology(newTopology);
         setRebalanceStatus();
      }
   }

   public void broadcastConsistentHashUpdate() throws Exception {
      CacheTopology cacheTopology = getCacheTopology();
      log.debugf("Updating cluster-wide consistent hash for cache %s, topology = %s", cacheName, cacheTopology);
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                                                                  CacheTopologyControlCommand.Type.CH_UPDATE, transport.getAddress(), cacheTopology,
                                                                  transport.getViewId());
      executeOnClusterAsync(command, getGlobalTimeout());
   }

   public void executeOnClusterAsync(final ReplicableCommand command, final int timeout)
         throws Exception {
      if (!isTotalOrder()) {
         // invoke the command on the local node
         asyncTransportExecutor.submit(new Runnable() {
            @Override
            public void run() {
               gcr.wireDependencies(command);
               try {
                  if (log.isTraceEnabled()) log.tracef("Attempting to execute command on self: %s", command);
                  command.perform(null);
               } catch (Throwable throwable) {
                  // The command already logs any exception in perform()
               }
            }
         });
      }

      // invoke remotely
      transport.invokeRemotely(null, command,
                               ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING, timeout, true, null, isTotalOrder(), isDistributed());
   }

   private int getGlobalTimeout() {
      // TODO Rename setting to something like globalRpcTimeout
      return (int) globalConfiguration.transport().distributedSyncTimeout();
   }

   /**
    * @return {@code true} if the topology was changed, {@code false} otherwise
    */
   private boolean updateTopologyAfterMembershipChange() {
      synchronized (this) {
         ConsistentHashFactory consistentHashFactory = getJoinInfo().getConsistentHashFactory();
         int topologyId = getCacheTopology().getTopologyId();
         ConsistentHash currentCH = getCacheTopology().getCurrentCH();
         ConsistentHash pendingCH = getCacheTopology().getPendingCH();
         if (!needConsistentHashUpdate()) {
            log.tracef("Cache %s members list was updated, but the cache topology doesn't need to change: %s",
                       cacheName, getCacheTopology());
            return false;
         }

         List<Address> newCurrentMembers = pruneInvalidMembers(currentCH.getMembers());
         if (newCurrentMembers.isEmpty()) {
            CacheTopology newTopology = new CacheTopology(topologyId + 1, null, null);
            updateCacheTopology(newTopology);
            log.tracef("Initial topology installed for cache %s: %s", cacheName, newTopology);
            return false;
         }
         ConsistentHash newCurrentCH = consistentHashFactory.updateMembers(currentCH, newCurrentMembers, getCapacityFactors());
         ConsistentHash newPendingCH = null;
         if (pendingCH != null) {
            List<Address> newPendingMembers = pruneInvalidMembers(pendingCH.getMembers());
            newPendingCH = consistentHashFactory.updateMembers(pendingCH, newPendingMembers, getCapacityFactors());
         }
         CacheTopology newTopology = new CacheTopology(topologyId + 1, newCurrentCH, newPendingCH);
         updateCacheTopology(newTopology);
         log.tracef("Cache %s topology updated: %s", cacheName, newTopology);
         newTopology.logRoutingTableInformation();
         return true;
      }
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
            ", members=" + members +
            ", joiners=" + joiners +
            ", cacheTopology=" + cacheTopology +
            ", rebalanceStatus=" + rebalanceStatus +
            '}';
   }

   public synchronized void reconcileCacheTopology(List<Address> clusterMembers, List<CacheTopology> partitionTopologies,
                                                   boolean isMergeView) throws Exception {
      try {
         if (partitionTopologies.isEmpty())
            return;


         CacheTopology cacheTopology;
         if (isMergeView) {
            cacheTopology = buildCacheTopologyForMerge(clusterMembers, partitionTopologies);
         }  else  {
            cacheTopology = buildCacheTopology(clusterMembers, partitionTopologies);
         }
         if (cacheTopology == null) return;

         // End any running rebalance
         if (isRebalanceInProgress()) {
            setRebalanceStatus();
         }
         updateCacheTopology(cacheTopology);

         // End any rebalance that was running in the other partitions
         broadcastConsistentHashUpdate();

         // Trigger another rebalance in case the CH is not balanced
         rebalancePolicy.updateCacheStatus(cacheName, this);
      } catch (Exception e) {
         log.failedToRecoverCacheState(cacheName, e);
      }

   }

   private CacheTopology buildCacheTopologyForMerge(List<Address> clusterMembers, List<CacheTopology> partitionTopologies) {
      log.trace("Building cache topology for merge.");
      int maxTopology = 0;
      // We only use the currentCH, we ignore any ongoing rebalance in the partitions
      ConsistentHash agreedCh = null;
      ConsistentHashFactory chFactory = getJoinInfo().getConsistentHashFactory();
      for (CacheTopology topology : partitionTopologies) {
         if (topology.getTopologyId() > maxTopology) {
            maxTopology = topology.getTopologyId();
            agreedCh = topology.getCurrentCH(); //todo shouldn't I use topology.getPendingCh() here, in case that cluster is moving towards a certain new topology?
         }
      }

      // We have added each node to the cache status when we received its status response
      // Prune those that have left the cluster.
      updateClusterMembers(clusterMembers);
      List<Address> members = getMembers();
      if (members.isEmpty()) {
         log.tracef("Cache %s has no members left, skipping topology update", cacheName);
         return null;
      }
      if (agreedCh != null) {
         agreedCh = chFactory.updateMembers(agreedCh, members, getCapacityFactors());
      }

      // Make sure the topology id is higher than any topology id we had before in the cluster
      maxTopology += 2;
      CacheTopology cacheTopology = new CacheTopology(maxTopology, agreedCh, null);
      return cacheTopology;

   }

   private CacheTopology buildCacheTopology(List<Address> clusterMembers, List<CacheTopology> partitionTopologies) {
      int unionTopologyId = 0;
      // We only use the currentCH, we ignore any ongoing rebalance in the partitions
      ConsistentHash currentCHUnion = null;
      ConsistentHashFactory chFactory = getJoinInfo().getConsistentHashFactory();
      for (CacheTopology topology : partitionTopologies) {
         if (topology.getTopologyId() > unionTopologyId) {
            unionTopologyId = topology.getTopologyId();
         }

         if (currentCHUnion == null) {
            currentCHUnion = topology.getCurrentCH();
         } else {
            currentCHUnion = chFactory.union(currentCHUnion, topology.getCurrentCH());
         }
      }

      // We have added each node to the cache status when we received its status response
      // Prune those that have left the cluster. //todo still do it to update removed nodes
      updateClusterMembers(clusterMembers);
      List<Address> members = getMembers();
      if (members.isEmpty()) {
         log.tracef("Cache %s has no members left, skipping topology update", cacheName);
         return null;
      }
      if (currentCHUnion != null) {
         //todo update if membership changed (result of updateClusterMembers above)
         currentCHUnion = chFactory.updateMembers(currentCHUnion, members, getCapacityFactors());
      }

      // Make sure the topology id is higher than any topology id we had before in the cluster
      unionTopologyId += 2;
      CacheTopology cacheTopology = new CacheTopology(unionTopologyId, currentCHUnion, null);
      return cacheTopology;
   }

   public synchronized void reconcileCacheTopologyWhenBecomingCoordinator(List<Address> clusterMembers, List<CacheTopology> partitionTopologies, boolean mergeView) throws Exception {
      log.tracef("reconcileCacheTopologyWhenBecomingCoordinator: members = %s, topologies = %s, mergeView = %s", clusterMembers, partitionTopologies, mergeView);
      if (partitionHandlingManager == null || partitionHandlingManager.handleViewChange(clusterMembers, this)) {
         reconcileCacheTopology(clusterMembers, partitionTopologies, mergeView);
      }
   }

   public String getCacheName() {
      return cacheName;
   }
}
