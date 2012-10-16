/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.transaction;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.MembershipArithmetic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
* Class responsible of cleaning transactions from the transaction table when nodes leave the cluster.
*
* @author Mircea Markus
* @since 5.1
*/
@Listener
public class StaleTransactionCleanupService {

   private static Log log = LogFactory.getLog(StaleTransactionCleanupService.class);


   private TransactionTable transactionTable;

   public StaleTransactionCleanupService(TransactionTable transactionTable) {
      this.transactionTable = transactionTable;
   }

   private ExecutorService lockBreakingService; // a thread pool with max. 1 thread

   /**
    * Roll back remote transactions that have acquired lock that are no longer valid,
    * because the main data owner left. Also unlocks keys for which the lock owner has changed as a result of a topology change.
    * This method will only ever be called in distributed mode.
    */
   @TopologyChanged
   @SuppressWarnings("unused")
   public void onTopologyChange(TopologyChangedEvent<?, ?> tce) {
      // Roll back remote transactions originating on nodes that have left the cluster.
      if (tce.isPre()) {
         ConsistentHash consistentHashAtStart = tce.getConsistentHashAtStart();
         if (consistentHashAtStart != null) {
            List<Address> leavers = MembershipArithmetic.getMembersLeft(consistentHashAtStart.getMembers(), tce.getConsistentHashAtEnd().getMembers());
            if (!leavers.isEmpty()) {
               log.tracef("Saw %d leavers - kicking off a lock breaking task", leavers.size());
               cleanTxForWhichTheOwnerLeft(leavers);
            }
         }
      }
   }

   private void cleanTxForWhichTheOwnerLeft(final Collection<Address> leavers) {
      try {
         lockBreakingService.submit(new Runnable() {
            @Override
            public void run() {
               try {
                  transactionTable.updateStateOnNodesLeaving(leavers);
               } catch (Exception e) {
                  log.error("Exception whilst updating state", e);
               }
            }
         });
      } catch (RejectedExecutionException ree) {
         log.error("Unable to submit task to executor", ree);
      }
   }

   public void start(final String cacheName, final RpcManager rpcManager, InterceptorChain interceptorChain, boolean isDistributed) {
      ThreadFactory tf = new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            String address = rpcManager != null ? rpcManager.getTransport().getAddress().toString() : "local";
            Thread th = new Thread(r, "LockBreakingService," + cacheName + "," + address);
            th.setDaemon(true);
            return th;
         }
      };
      lockBreakingService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(), tf,
                                                   new ThreadPoolExecutor.DiscardOldestPolicy());
   }

   public void stop() {
      if (lockBreakingService != null)
         lockBreakingService.shutdownNow();
   }
}
