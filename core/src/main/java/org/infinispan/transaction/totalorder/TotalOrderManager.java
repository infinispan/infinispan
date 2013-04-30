/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.transaction.totalorder;

import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.TotalOrderRemoteTransactionState;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class behaves as a synchronization point between incoming transactions (totally ordered) and between incoming
 * transactions and state transfer.
 * <p/>
 * Main functions:
 * <ul>
 *    <li>
 *       ensure an order between prepares before sending them to the thread pool, i.e. non-conflicting
 * prepares can be processed concurrently;
 *    </li>
 *    <li>
 *       ensure that the state transfer waits for the previous delivered prepares;
 *    </li>
 *    <li>
 *       ensure that the prepare waits for state transfer in progress.
 *    </li>
 * </ul>
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderManager {

   private static final Log log = LogFactory.getLog(TotalOrderManager.class);
   /**
    * this map is used to keep track of concurrent transactions.
    */
   private final ConcurrentMap<Object, TotalOrderLatch> keysLocked;
   private final AtomicReference<TotalOrderLatch> clear;
   private final AtomicReference<TotalOrderLatch> stateTransferInProgress;
   private BlockingTaskAwareExecutorService totalOrderExecutor;

   public TotalOrderManager() {
      keysLocked = CollectionFactory.makeConcurrentMap();
      clear = new AtomicReference<TotalOrderLatch>(null);
      stateTransferInProgress = new AtomicReference<TotalOrderLatch>(null);
   }

   @Inject
   public void inject(@ComponentName(KnownComponentNames.TOTAL_ORDER_EXECUTOR) BlockingTaskAwareExecutorService totalOrderExecutor) {
      this.totalOrderExecutor = totalOrderExecutor;
   }

   /**
    * It ensures the validation order for the transaction corresponding to the prepare command. This allow the prepare
    * command to be moved to a thread pool.
    *
    * @param state the total order prepare state
    */
   public final void ensureOrder(TotalOrderRemoteTransactionState state, Object[] keysModified) throws InterruptedException {
      //the retries due to state transfer re-uses the same state. we need that the keys previous locked to be release
      //in order to insert it again in the keys locked.
      //NOTE: this method does not need to be synchronized because it is invoked by a one thread at the time, namely
      //the thread that is delivering the messages in total order.
      state.awaitUntilReset();
      TotalOrderLatch transactionSynchronizedBlock = new TotalOrderLatchImpl(state.getGlobalTransaction().globalId());
      state.setTransactionSynchronizedBlock(transactionSynchronizedBlock);
      if (keysModified == null) { //clear state
         TotalOrderLatch oldClear = clear.get();
         if (oldClear != null) {
            state.addSynchronizedBlock(oldClear);
            clear.set(transactionSynchronizedBlock);
         }
         //add all other "locks"
         state.addAllSynchronizedBlocks(keysLocked.values());
         keysLocked.clear();
         state.addKeysLockedForClear();
      } else {
         TotalOrderLatch clearTx = clear.get();
         if (clearTx != null) {
            state.addSynchronizedBlock(clearTx);
         }
         //this will collect all the count down latch corresponding to the previous transactions in the queue
         for (Object key : keysModified) {
            TotalOrderLatch prevTx = keysLocked.put(key, transactionSynchronizedBlock);
            if (prevTx != null) {
               state.addSynchronizedBlock(prevTx);
            }
            state.addLockedKey(key);
         }
      }

      TotalOrderLatch stateTransfer = stateTransferInProgress.get();
      if (stateTransfer != null) {
         state.addSynchronizedBlock(stateTransfer);
      }

      if (log.isTraceEnabled()) {
         log.tracef("Transaction [%s] will wait for %s and locked %s", state.getGlobalTransaction().globalId(),
                    state.getConflictingTransactionBlocks(), state.getLockedKeys() == null ? "[ClearCommand]" :
               state.getLockedKeys());
      }
   }

   /**
    * Release the locked key possibly unblock waiting prepares.
    *
    * @param state the state
    */
   public final void release(TotalOrderRemoteTransactionState state) {
      TotalOrderLatch synchronizedBlock = state.getTransactionSynchronizedBlock();
      if (synchronizedBlock == null) {
         //already released!
         return;
      }
      Collection<Object> lockedKeys = state.getLockedKeys();
      synchronizedBlock.unBlock();
      if (lockedKeys == null) {
         clear.compareAndSet(synchronizedBlock, null);
      } else {
         for (Object key : lockedKeys) {
            keysLocked.remove(key, synchronizedBlock);
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Release %s and locked keys %s. Checking pending tasks!", synchronizedBlock,
                    lockedKeys == null ? "[ClearCommand]" : lockedKeys);
      }
      state.reset();
   }

   /**
    * It notifies that a state transfer is about to start.
    *
    * @param topologyId the new topology ID
    * @return the current pending prepares
    */
   public final Collection<TotalOrderLatch> notifyStateTransferStart(int topologyId) {
      List<TotalOrderLatch> preparingTransactions = new ArrayList<TotalOrderLatch>(keysLocked.size());
      preparingTransactions.addAll(keysLocked.values());
      TotalOrderLatch clearBlock = clear.get();
      if (clearBlock != null) {
         preparingTransactions.add(clearBlock);
      }
      if (stateTransferInProgress.get() == null) {
         stateTransferInProgress.set(new TotalOrderLatchImpl("StateTransfer-" + topologyId));
      }
      if (log.isTraceEnabled()) {
         log.tracef("State Transfer start. It will wait for %s", preparingTransactions);
      }
      return preparingTransactions;
   }

   /**
    * It notifies the end of the state transfer possibly unblock waiting prepares.
    */
   public final void notifyStateTransferEnd() {
      TotalOrderLatch block = stateTransferInProgress.getAndSet(null);
      if (block != null) {
         block.unBlock();
      }
      if (log.isTraceEnabled()) {
         log.tracef("State Transfer finish. It will release %s", block);
      }
      totalOrderExecutor.checkForReadyTasks();
   }
}
