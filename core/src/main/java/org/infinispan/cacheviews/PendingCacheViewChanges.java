/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.cacheviews;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is used on the coordinator to keep track of changes since the last merge.
 * 
 * When the coordinator changes or in case of a merge, the new coordinator recovers the last committed view
 * from all the members and rolls back any uncommitted views, then it prepares a new view if necessary.
 */
public class PendingCacheViewChanges {
   private static final Log log = LogFactory.getLog(PendingCacheViewChanges.class);

   private final Object lock = new Object();

   private final String cacheName;

   // The last view id generated (or received during a recover operation)
   private int lastViewId;
   // The join requests since the last COMMIT_VIEW
   // These are only used if we are the coordinator.
   private final Set<Address> joiners;
   // The leave requests are also used on normal nodes to compute the valid members set
   private final Set<Address> leavers;
   // True if there was a merge since the last committed view
   private Set<Address> membersAfterCoordChange;

   private boolean viewInstallationInProgress;

   public PendingCacheViewChanges(String cacheName) {
      this.cacheName = cacheName;
      this.joiners = new HashSet<Address>();
      this.leavers = new HashSet<Address>();
      this.membersAfterCoordChange = Collections.emptySet();
   }

   /**
    * Called on the coordinator to create the view that will be prepared next.
    * It also sets the pendingView field, so the next call to {@code prepareView} will have no effect.
    */
   public CacheView createPendingView(CacheView committedView) {
      synchronized (lock) {
         // TODO Enforce view installation policy here?
         if (viewInstallationInProgress) {
            log.tracef("Cannot prepare a new view, there is another view installation in progress");
            return null;
         }
         if (leavers.size() == 0 && joiners.size() == 0 && membersAfterCoordChange == null) {
            log.tracef("Cannot prepare a new view, we have no joiners or leavers");
            return null;
         }

         Collection<Address> baseMembers = membersAfterCoordChange != null ? membersAfterCoordChange : committedView.getMembers();
         List<Address> members = new ArrayList<Address>(baseMembers);
         // If a node is both in leavers and in joiners we should install a view without it first
         // so that other nodes don't consider it an old owner, so we first add it as a joiner
         // and then we remove it as a leaver.
         members.addAll(joiners);
         members.removeAll(leavers);

         viewInstallationInProgress = true;

         lastViewId++;
         CacheView pendingView = new CacheView(lastViewId, members);
         log.tracef("%s: created new view %s", cacheName, pendingView);
         return pendingView;
      }
   }

   /**
    * Called on the coordinator before a rollback to assign a unique view id to the rollback.
    */
   public int getRollbackViewId() {
      synchronized (lock) {
         lastViewId++;
         return lastViewId;
      }
   }

   public boolean hasChanges() {
      return membersAfterCoordChange != null || !joiners.isEmpty() || !leavers.isEmpty();
   }

   public void resetChanges(CacheView committedView) {
      synchronized (lock) {
         // the list of valid members remains the same
         if (log.isDebugEnabled()) {
            // if a node was both a joiner and a leaver, the committed view should not contain it
            List<Address> bothJoinerAndLeavers = new ArrayList<Address>(joiners);
            bothJoinerAndLeavers.retainAll(leavers);
            for (Address node : bothJoinerAndLeavers) {
               if (committedView.getMembers().contains(node)) {
                  log.debugf("Node %s should not be a member in view %s, left and then joined before the view was installed");
               }
            }
         }
         leavers.retainAll(committedView.getMembers());
         joiners.removeAll(committedView.getMembers());
         membersAfterCoordChange = null;

         viewInstallationInProgress = false;
         if (committedView.getViewId() > lastViewId) {
            lastViewId = committedView.getViewId();
         }
      }
   }

   /**
    * Signal a join
    */
   public void requestJoin(Address joiner) {
      synchronized (lock) {
         log.tracef("%s: Node %s is joining", cacheName, joiner);
         // if the node wanted to leave earlier, we don't remove it from the list of leavers
         // since it has already left, it won't have the latest data and so it's not a valid member
         joiners.add(joiner);
      }
   }

   /**
    * Signal a leave.
    */
   public Set<Address> requestLeave(Collection<Address> leavers) {
      synchronized (lock) {
         log.tracef("%s: Nodes %s are leaving", cacheName, leavers);
         // if the node wanted to join earlier, just remove it from the list of joiners
         Set<Address> leavers2 = new HashSet<Address>(leavers);
         // removeAll will also remove any joiners from leavers2
         joiners.removeAll(leavers2);
         log.tracef("%s: After pruning nodes that have joined but have never installed a view, leavers are %s", cacheName, leavers2);

         this.leavers.addAll(leavers2);
         return leavers2;
      }
   }

   /**
    * Signal a merge
    */
   public void requestCoordChange(Collection<Address> newMembers, Collection<Address> joiners) {
      synchronized (lock) {
         log.tracef("%s: Coordinator changed, this node is the current coordinator", cacheName);
         membersAfterCoordChange = new HashSet<Address>(newMembers);
         // Apply any changes that we may have received before we realized we're the coordinator
         membersAfterCoordChange.removeAll(leavers);
         joiners.removeAll(membersAfterCoordChange);
         log.tracef("%s: Members after coordinator change: %s, joiners: %s, leavers: %s", cacheName, membersAfterCoordChange, joiners, leavers);
      }
   }

   /**
    * If we recovered a view after a merge or coordinator change we need to make sure the next view id is greater
    * than any view id that was already committed.
    */
   public void updateLatestViewId(int viewId) {
      synchronized (lock) {
         if (viewId > lastViewId) {
            lastViewId = viewId;
         }
      }
   }

   /**
    * @return the nodes that left since the last {@code resetChanges} call
    */
   public Set<Address> getLeavers() {
      synchronized (lock) {
         return Immutables.immutableSetCopy(leavers);
      }
   }

   /**
    * @return true if {@code createPendingView} has been called without a pair {@code resetChanges}
    */
   public boolean isViewInstallationInProgress() {
      return viewInstallationInProgress;
   }

   /**
    * @return the id of the view we created last (or received via {@code updateLatestViewId}.
    */
   public int getLastViewId() {
      synchronized (lock) {
         return lastViewId;
      }
   }
}
