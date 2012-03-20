package org.infinispan.transaction.totalOrder;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.*;

/**
 * The state of a Remote Transaction to be used in Total Order protocol. It behaves as a synchronization point between
 * commit/rollback command and the prepare command
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderState {

   private enum State {
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

   private TxDependencyLatch latch;
   private EnumSet<State> state;

   public TotalOrderState(GlobalTransaction gtx) {
      this.state = EnumSet.noneOf(State.class);
      this.latch = new TxDependencyLatch(gtx);
   }

   /**
    * check if the transaction is marked for rollback (by the Rollback Command)
    * @return true if it is marked for rollback, false otherwise
    */
   public synchronized boolean isMarkedForRollback() {
      return state.contains(State.ROLLBACK_ONLY);
   }

   /**
    * check if the transaction is marked for commit (by the Commit Command)
    * @return true if it is marked for commit, false otherwise
    */
   public synchronized boolean isMarkedForCommit() {
      return state.contains(State.COMMIT_ONLY);
   }

   /**
    * mark the transaction as prepared (the validation was finished) and notify a possible pending commit or rollback
    * command
    */
   public synchronized void markPreparedAndNotify() {
      state.add(State.PREPARED);
      this.notifyAll();
   }

   /**
    * mark the transaction as preparing, blocking the commit and rollback commands until the
    * {@link #markPreparedAndNotify()} is invoked
    */
   public synchronized void markForPreparing() {
      state.add(State.PREPARING);
   }

   /**
    * Commit and rollback commands invokes this method and they are blocked here if the state is PREPARING
    *
    * @param commit true if it is a commit command, false otherwise
    * @return true if the command needs to be processed, false otherwise
    * @throws InterruptedException when it is interrupted while waiting
    */
   public synchronized boolean waitPrepared(boolean commit) throws InterruptedException {
      if (state.contains(State.PREPARED)) {
         return true;
      }
      if (state.contains(State.PREPARING)) {
         this.wait();
         return true;
      }
      state.add(commit ? State.COMMIT_ONLY : State.ROLLBACK_ONLY);
      return false;
   }

   /**
    * Gets a set of the modified keys from a collection of write commands
    * @param modifications the collection of write commands
    * @return a set of keys
    */
   public static Set<Object> getModifiedKeys(Collection<WriteCommand> modifications) {
      if (modifications == null || modifications.size() == 0) {
         return Collections.emptySet();
      }

      Set<Object> keys = new HashSet<Object>();
      for (WriteCommand writeCommand : modifications) {
         keys.addAll(writeCommand.getAffectedKeys());
      }

      return keys;
   }

   /**
    * Gets the latch associated to this remote transaction
    * @return the latch associated to this transaction
    */
   public TxDependencyLatch getLatch() {
      return latch;
   }

   @Override
   public String toString() {
      return "TotalOrderState{" +
            "latch=" + latch +
            ", state=" + state +
            '}';
   }
}
