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

package org.infinispan.statetransfer;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Holds a map of push confirmations and a map of join confirmations.
 * <p/>
 * When a node starts up and receives the initial view, both maps are empty.
 * It then sends a join request, which allows the old nodes to send state.
 * As the old nodes send state and the new node starts receiving push confirmations,
 * it fills both the push confirmations map and the join confirmations map.
 * When the join confirmations map is full, the node will send its own push confirmation.
 * <p/>
 * When a running node receives a new view, it waits until it gets a join request from
 * all the new nodes in the view. Only then it starts sending state.
 * <p/>
 * When a running node receives a merge view, it knows that all the members of the view
 * are also up and running. So it doesn't wait for a join request from any of them.
 */
class PushConfirmationsMap {
   private static final Log log = LogFactory.getLog(PushConfirmationsMap.class);

   private final Lock lock = new ReentrantLock();
   private final Condition clusterCompletedPush = lock.newCondition();
   private final Condition clusterConfirmedJoin = lock.newCondition();
   private final Map<Address, Integer> pushConfirmations = new HashMap<Address, Integer>();
   private final Map<Address, Integer> joinConfirmations = new HashMap<Address, Integer>();
   private int lastViewId = -1;
   private int membersCount = Integer.MAX_VALUE;
   private int actualConfirmationsCount = 0;

   public void confirmJoin(Address sender, int viewId) {
      lock.lock();
      try {
         if (!joinConfirmations.containsKey(sender)) {
            joinConfirmations.put(sender, viewId);
         }
         if (joinConfirmations.size() >= membersCount) {
            clusterConfirmedJoin.signalAll();
         }
      } finally {
         lock.unlock();
      }
   }

   public void confirmPush(Address sender, int viewId) {
      lock.lock();
      try {
         confirmJoin(sender, viewId);

         if (viewId < lastViewId) {
            log.debugf("Ignoring outdated push confirmation from %s for old view id %d (last view id is %d)", sender, viewId, lastViewId);
            return;
         }

         if (viewId > lastViewId) {
            newViewReceived(viewId, null, false);
         }
         pushConfirmations.put(sender, viewId);
         actualConfirmationsCount++;

         log.tracef("Received push confirmation from %s for view %d, confirmed %d of %d, confirmations map is %s",
               sender, viewId, actualConfirmationsCount, membersCount, pushConfirmations);
         if (actualConfirmationsCount == membersCount) {
            clusterCompletedPush.signalAll();
         }
      } finally {
         lock.unlock();
      }
   }

   public void initialViewReceived(int viewId, List<Address> members) {
      lock.lock();
      try {
         lastViewId = viewId;
         membersCount = members.size();
         actualConfirmationsCount = 0;
      } finally {
         lock.unlock();
      }
   }

   public void newViewReceived(int viewId, List<Address> members, boolean isMerge) {
      lock.lock();
      try {
         if (viewId < lastViewId) {
            log.tracef("Got a new view %d after a push confirmation message for that view, ignoring it. Last view is %d",
                  viewId, lastViewId);
            return;
         } else if (viewId > lastViewId) {
            lastViewId = viewId;
            membersCount = members != null ? members.size() : Integer.MAX_VALUE;
            actualConfirmationsCount = 0;
            log.tracef("Received new view %d, confirmed %d of %d",
                  viewId, actualConfirmationsCount, membersCount);
         } else if (viewId == lastViewId && membersCount == Integer.MAX_VALUE) {
            log.tracef("Received proper members list for cluster view %d %s", viewId, members);
            membersCount = members.size();
         }
         if (members != null) {
            if (isMerge) {
               for (Address member : members) {
                  if (!joinConfirmations.containsKey(member)) {
                     joinConfirmations.put(member, viewId);
                  }
               }
            } else {
               joinConfirmations.keySet().retainAll(members);
            }
            pushConfirmations.keySet().retainAll(members);
            log.tracef("Updated confirmed members are %s, push confirmations map is %s",
                  joinConfirmations, pushConfirmations);
         }

         // wake up the rehasher thread if it's been waiting for the other members to do something
         clusterConfirmedJoin.signalAll();
         clusterCompletedPush.signalAll();
      } finally {
         lock.unlock();
      }
   }

   /**
    * @return <code>true</code> if the wait is successful, and <code>false</code> if another view has arrived before
    *    all the members of the cluster confirmed pushing their state.
    * @throws InterruptedException
    */
   public boolean waitForClusterToCompletePush(int viewId, long timeout) throws InterruptedException, TimeoutException {
      log.tracef("Waiting for all the members of the cluster to confirm pushing state for view %d, received confirmations %s",
            viewId, pushConfirmations);
      lock.lock();
      try {
         if (viewId == lastViewId && actualConfirmationsCount < membersCount) {
            clusterCompletedPush.await(timeout, TimeUnit.MILLISECONDS);
         }

         if (actualConfirmationsCount == membersCount) {
            log.tracef("Push confirmed by all cluster members");
            return true;
         }

         if (viewId < lastViewId) {
            log.tracef("Received new view %d while waiting for push confirmations for view %d", lastViewId, viewId);
            return false;
         }

         log.stateTransferTimeoutWaitingForPushConfirmations(viewId, pushConfirmations);
         throw new TimeoutException("Timed out waiting for all cluster members to confirm pushing data");
      } finally {
         lock.unlock();
      }
   }

   /**
    * @return <code>true</code> if the wait is successful, and <code>false</code> if another view has arrived before
    *    all the members of the cluster confirmed joining the cluster.
    * @throws InterruptedException
    */
   public boolean waitForClusterToConfirmJoin(int viewId, long timeout) throws InterruptedException, TimeoutException {
      log.tracef("Waiting for all the members of the cluster to confirm joining for view %d, confirmed members %s",
            viewId, joinConfirmations);
      lock.lock();
      try {
         if (viewId == lastViewId && joinConfirmations.size() < membersCount) {
            clusterConfirmedJoin.await(timeout, TimeUnit.MILLISECONDS);
         }

         if (joinConfirmations.size() == membersCount) {
            log.tracef("Join confirmed by all cluster members");
            return true;
         }

         if (viewId < lastViewId) {
            log.tracef("Received new view %d while waiting for join confirmations for view %d", lastViewId, viewId);
            return false;
         }

         log.stateTransferTimeoutWaitingForJoinConfirmations(viewId, joinConfirmations);
         throw new TimeoutException("Timed out waiting for all cluster members to confirm joining");
      } finally {
         lock.unlock();
      }
   }
}
