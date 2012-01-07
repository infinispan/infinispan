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
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
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
   private InterceptorChain invoker;

   public StaleTransactionCleanupService(TransactionTable transactionTable) {
      this.transactionTable = transactionTable;
   }

   private ExecutorService lockBreakingService;

   /**
    * Roll back remote transactions originating on nodes that have left the cluster.
    */
   @ViewChanged
   public void onViewChange(ViewChangedEvent vce) {
      final List<Address> leavers = MembershipArithmetic.getMembersLeft(vce.getOldMembers(),
                                                                        vce.getNewMembers());
      if (!leavers.isEmpty()) {
         log.tracef("Saw %d leavers - kicking off a lock breaking task", leavers.size());
         cleanTxForWhichTheOwnerLeft(leavers);
      }
   }

   /**
    * Roll back remote transactions that have acquired lock that are no longer valid,
    * either because the main data owner left the cluster or because a node joined
    * the cluster and is the new data owner.
    * This method will only ever be called in distributed mode.
    */
   @TopologyChanged
   public void onTopologyChange(TopologyChangedEvent tce) {
      // do all the work AFTER the consistent hash has changed
      if (tce.isPre())
         return;

      Address self = transactionTable.rpcManager.getAddress();
      ConsistentHash chOld = tce.getConsistentHashAtStart();
      ConsistentHash chNew = tce.getConsistentHashAtEnd();

      // for remote transactions, release locks for which we are no longer an owner
      // only for remote transactions, since we acquire locks on the origin node regardless if it's the owner or not
      log.tracef("Unlocking keys for which we are no longer an owner");
      int numOwners = transactionTable.configuration.getNumOwners();
      for (RemoteTransaction remoteTx : transactionTable.getRemoteTransactions()) {
         GlobalTransaction gtx = remoteTx.getGlobalTransaction();
         List<Object> keys = new ArrayList<Object>();
         boolean txHasLocalKeys = false;
         for (Object key : remoteTx.getLockedKeys()) {
            boolean wasLocal = chOld.isKeyLocalToAddress(self, key, numOwners);
            boolean isLocal = chNew.isKeyLocalToAddress(self, key, numOwners);
            if (wasLocal && !isLocal) {
               keys.add(key);
            }
            txHasLocalKeys |= isLocal;
         }
         if (keys.size() > 0) {
            log.tracef("Unlocking keys %s for remote transaction %s as we are no longer an owner", keys, gtx);
            Set<Flag> flags = EnumSet.of(Flag.CACHE_MODE_LOCAL);
            String cacheName = transactionTable.configuration.getName();
            LockControlCommand unlockCmd = new LockControlCommand(keys, cacheName, flags, gtx);
            unlockCmd.init(invoker, transactionTable.icc, transactionTable);
            unlockCmd.setUnlock(true);
            try {
               unlockCmd.perform(null);
               log.tracef("Unlocking moved keys for %s complete.", gtx);
            } catch (Throwable t) {
               log.unableToUnlockRebalancedKeys(gtx, keys, self, t);
            }

            // if the transaction doesn't touch local keys any more, we can roll it back
            if (!txHasLocalKeys) {
               log.tracef("Killing remote transaction without any local keys %s", gtx);
               RollbackCommand rc = new RollbackCommand(cacheName, gtx);
               rc.init(invoker, transactionTable.icc, transactionTable);
               try {
                  rc.perform(null);
                  log.tracef("Rollback of transaction %s complete.", gtx);
               } catch (Throwable e) {
                  log.unableToRollbackGlobalTx(gtx, e);
               } finally {
                  transactionTable.removeRemoteTransaction(gtx);
               }
            }
         }
      }

      log.trace("Finished cleaning locks for keys that are no longer local");
   }

   private void cleanTxForWhichTheOwnerLeft(final Collection<Address> leavers) {
      try {
         lockBreakingService.submit(new Runnable() {
            public void run() {
               try {
               transactionTable.updateStateOnNodesLeaving(leavers);
               } catch (Exception e) {
                  log.error("Exception whilst updating state", e);
               }
            }
         });
      } catch (RejectedExecutionException ree) {
         log.debug("Unable to submit task to executor", ree);
      }
   }

   public void start(final Configuration configuration, final RpcManager rpcManager, InterceptorChain interceptorChain) {
      this.invoker = interceptorChain;
      ThreadFactory tf = new ThreadFactory() {
         public Thread newThread(Runnable r) {
            String address = rpcManager != null ? rpcManager.getTransport().getAddress().toString() : "local";
            Thread th = new Thread(r, "LockBreakingService," + configuration.getName() + "," + address);
            th.setDaemon(true);
            return th;
         }
      };
      lockBreakingService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(), tf,
                                                   new ThreadPoolExecutor.CallerRunsPolicy());
   }

   public void stop() {
      if (lockBreakingService != null)
         lockBreakingService.shutdownNow();
   }
}
