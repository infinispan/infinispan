package org.infinispan.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
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

   public ClusterCacheStatus(String cacheName, CacheJoinInfo joinInfo) {
      this.cacheName = cacheName;
      this.joinInfo = joinInfo;

      this.cacheTopology = new CacheTopology(-1, null, null);
      this.members = InfinispanCollections.emptyList();
      this.capacityFactors = InfinispanCollections.emptyMap();
      this.joiners = InfinispanCollections.emptyList();
      if (trace) log.tracef("Cache %s initialized, join info is %s", cacheName, joinInfo);
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
         newCapacityFactors.put(joiner, joinInfo.getCapacityFactor());
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

   public void endRebalance() {
      synchronized (this) {
         if (rebalanceStatus == null) {
            throw new IllegalStateException("Can't end rebalance, there is no rebalance in progress");
         }
         rebalanceStatus = null;
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
}
