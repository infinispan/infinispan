/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;
import org.infinispan.util.InfinispanCollections;

/**
* Keeps track of a cache's status: members, current/pending consistent hashes, and rebalance status
*
* @author Dan Berindei
* @since 5.2
*/
class ClusterCacheStatus {
   private final String cacheName;
   private final CacheJoinInfo joinInfo;
   // Cache members, some of which may not have received state yet
   private volatile List<Address> members;
   // Cache members that have not yet received state
   private volatile List<Address> joiners;
   // Cache topology. Its consistent hashes contain only members that did receive/are receiving state
   private volatile CacheTopology cacheTopology;

   private volatile RebalanceConfirmationCollector rebalanceStatus;

   public ClusterCacheStatus(String cacheName, CacheJoinInfo joinInfo) {
      this.cacheName = cacheName;
      this.joinInfo = joinInfo;

      this.cacheTopology = new CacheTopology(-1, null, null);
      this.members = InfinispanCollections.emptyList();
      this.joiners = InfinispanCollections.emptyList();
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

   public void setMembers(List<Address> newMembers) {
      synchronized (this) {
         members = Immutables.immutableListCopy(newMembers);

         ConsistentHash currentCH = cacheTopology.getCurrentCH();
         if (currentCH != null) {
            joiners = immutableRemoveAll(members, currentCH.getMembers());
         } else {
            joiners = members;
         }
      }
   }

   /**
    * @return {@code true} if the joiner was not already a member, {@code false} otherwise
    */
   public boolean addMember(Address joiner) {
      synchronized (this) {
         if (members.contains(joiner))
            return false;

         members = immutableAdd(members, joiner);
         joiners = immutableAdd(joiners, joiner);
         return true;
      }
   }

   /**
    * @return {@code true} if the leaver was a member, {@code false} otherwise
    */
   public boolean removeMember(Address leaver) {
      synchronized (this) {
         if (!members.contains(leaver))
            return false;

         members = immutableRemove(members, leaver);
         joiners = immutableRemove(joiners, leaver);
         return true;
      }
   }

   /**
    * @return {@code true} if the members list has changed, {@code false} otherwise
    */
   public boolean updateClusterMembers(List<Address> newClusterMembers) {
      synchronized (this) {
         if (newClusterMembers.containsAll(members))
            return false;

         members = immutableRetainAll(members, newClusterMembers);
         joiners = immutableRetainAll(joiners, newClusterMembers);
         return true;
      }
   }

   public CacheTopology getCacheTopology() {
      return cacheTopology;
   }

   public void updateCacheTopology(CacheTopology newTopology) {
      synchronized (this) {
         this.cacheTopology = newTopology;

         // update the joiners list
         if (newTopology.getCurrentCH() != null) {
            joiners = immutableRemoveAll(joiners, newTopology.getCurrentCH().getMembers());
         }
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

   public void endRebalance(CacheTopology newTopology) {
      synchronized (this) {
         if (rebalanceStatus == null) {
            throw new IllegalStateException("Can't end rebalance, there is no rebalance in progress");
         }

         updateCacheTopology(newTopology);
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


}
