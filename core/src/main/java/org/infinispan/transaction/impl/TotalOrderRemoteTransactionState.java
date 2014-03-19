package org.infinispan.transaction.impl;

import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.transaction.totalorder.TotalOrderLatch;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

/**
 * Represents a state for a Remote Transaction when the Total Order based protocol is used.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderRemoteTransactionState {

   private static final Log log = LogFactory.getLog(TotalOrderRemoteTransactionState.class);
   private final EnumSet<State> transactionState;
   private final GlobalTransaction globalTransaction;
   private List<Object> lockedKeys;
   private TotalOrderLatch block;
   private List<TotalOrderLatch> dependencies;

   public TotalOrderRemoteTransactionState(GlobalTransaction globalTransaction) {
      this.transactionState = EnumSet.noneOf(State.class);
      this.globalTransaction = globalTransaction;
   }

   /**
    * check if the transaction is marked for rollback (by the Rollback Command)
    *
    * @return true if it is marked for rollback, false otherwise
    */
   public synchronized boolean isRollbackReceived() {
      return transactionState.contains(State.ROLLBACK_ONLY);
   }

   /**
    * check if the transaction is marked for commit (by the Commit Command)
    *
    * @return true if it is marked for commit, false otherwise
    */
   public synchronized boolean isCommitReceived() {
      return transactionState.contains(State.COMMIT_ONLY);
   }

   /**
    * mark the transaction as prepared (the validation was finished) and notify a possible pending commit or rollback
    * command
    */
   public synchronized void prepared() {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Current status is %s, setting status to: PREPARED", globalTransaction.globalId(),
                    transactionState);
      }
      transactionState.add(State.PREPARED);
      notifyAll();
   }

   /**
    * mark the transaction as preparing, blocking the commit and rollback commands until the {@link #prepared()} is
    * invoked
    */
   public synchronized void preparing() {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Current status is %s, setting status to: PREPARING", globalTransaction.globalId(),
                    transactionState);
      }
      transactionState.add(State.PREPARING);
   }

   /**
    * Commit and rollback commands invokes this method and they are blocked here if the state is PREPARING
    *
    * @param commit true if it is a commit command, false otherwise
    * @return true if the command needs to be processed, false otherwise
    * @throws InterruptedException when it is interrupted while waiting
    */
   public final synchronized boolean waitUntilPrepared(boolean commit)
         throws InterruptedException {
      boolean result;

      State state = commit ? State.COMMIT_ONLY : State.ROLLBACK_ONLY;
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Current status is %s, setting status to: %s", globalTransaction.globalId(),
                    transactionState, state);
      }
      transactionState.add(state);

      if (transactionState.contains(State.PREPARED)) {
         result = true;
         if (log.isTraceEnabled()) {
            log.tracef("[%s] Transaction is PREPARED", globalTransaction.globalId());
         }
      } else if (transactionState.contains(State.PREPARING)) {
         wait();
         result = true;
         if (log.isTraceEnabled()) {
            log.tracef("[%s] Transaction was in PREPARING state but now it is prepared", globalTransaction.globalId());
         }
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("[%s] Transaction is not delivered yet", globalTransaction.globalId());
         }
         result = false;
      }
      return result;
   }

   /**
    * @return true if the transaction has received the prepare and the commit or rollback
    */
   public final synchronized boolean isFinished() {
      return transactionState.contains(State.PREPARED) &&
            (transactionState.contains(State.COMMIT_ONLY) || transactionState.contains(State.ROLLBACK_ONLY));
   }

   /**
    * @return the keys locked in {@code org.infinispan.transaction.totalorder.TotalOrderManager}
    */
   public final synchronized Collection<Object> getLockedKeys() {
      return lockedKeys;
   }

   /**
    * @return the {@code TotalOrderLatch} associated to this transaction
    */
   public final synchronized TotalOrderLatch getTransactionSynchronizedBlock() {
      return block;
   }

   /**
    * Sets the {@code TotalOrderLatch} to be associated to this transaction
    */
   public final synchronized void setTransactionSynchronizedBlock(TotalOrderLatch block) {
      this.block = block;
   }

   /**
    * @return the global transaction
    */
   public final synchronized GlobalTransaction getGlobalTransaction() {
      return globalTransaction;
   }

   public final synchronized void awaitUntilReset() throws InterruptedException {
      while (block != null && lockedKeys != null) {
         wait();
      }
   }

   public final synchronized void reset() {
      this.block = null;
      this.lockedKeys = null;
      notifyAll();
   }

   @Override
   public String toString() {
      return "TotalOrderRemoteTransactionState{" +
            "transactionState=" + transactionState +
            ", globalTransaction='" + globalTransaction.globalId() + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TotalOrderRemoteTransactionState state = (TotalOrderRemoteTransactionState) o;

      return !(globalTransaction != null ? !globalTransaction.equals(state.globalTransaction) :
                     state.globalTransaction != null);

   }

   @Override
   public int hashCode() {
      return globalTransaction != null ? globalTransaction.hashCode() : 0;
   }

   public final synchronized void addSynchronizedBlock(TotalOrderLatch block) {
      if (dependencies == null) {
         dependencies = new ArrayList<TotalOrderLatch>(8);
      }
      dependencies.add(block);
   }

   public final synchronized void addAllSynchronizedBlocks(Collection<TotalOrderLatch> blocks) {
      if (dependencies == null) {
         dependencies = new ArrayList<TotalOrderLatch>(blocks);
      } else {
         dependencies.addAll(blocks);
      }
   }

   public final synchronized void addKeysLockedForClear() {
      lockedKeys = null;
   }

   public final synchronized void addLockedKey(Object key) {
      if (lockedKeys == null) {
         lockedKeys = new ArrayList<Object>(8);
      }
      lockedKeys.add(key);
   }

   public synchronized Collection<TotalOrderLatch> getConflictingTransactionBlocks() {
      return dependencies == null ? InfinispanCollections.<TotalOrderLatch>emptyList() : dependencies;
   }

   private static enum State {
      /**
       * the prepare command was received and started the validation
       */
      PREPARING,
      /**
       * the prepare command was received and finished the validation
       */
      PREPARED,
      /**
       * the rollback command was received before the prepare command and the transaction must be aborted
       */
      ROLLBACK_ONLY,
      /**
       * the commit command was received before the prepare command and the transaction must be committed
       */
      COMMIT_ONLY
   }
}
