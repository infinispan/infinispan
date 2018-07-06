package org.infinispan.transaction.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.InvalidTransactionException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Defines the state of a remotely originated transaction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoteTransaction extends AbstractCacheTransaction implements Cloneable {

   private static final Log log = LogFactory.getLog(RemoteTransaction.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final CompletableFuture<Void> INITIAL_FUTURE = CompletableFutures.completedNull();

   /**
    * This int should be set to the highest topology id for transactions received via state transfer. During state
    * transfer we do not migrate lookedUpEntries to save bandwidth. If lookedUpEntriesTopology is less than the
    * topology of the CommitCommand that is received this indicates the preceding PrepareCommand was received by
    * previous owner before state transfer or the data has now changed owners and the current owner
    * now has to re-execute prepare to populate lookedUpEntries (and acquire the locks).
    */
   // Default value of MAX_VALUE basically means it hasn't yet received what topology id this is for the entries
   private volatile int lookedUpEntriesTopology = Integer.MAX_VALUE;

   private volatile TotalOrderRemoteTransactionState transactionState;
   private final Object transactionStateLock = new Object();

   private final AtomicReference<CompletableFuture<Void>> synchronization =
         new AtomicReference<>(INITIAL_FUTURE);

   public RemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx, int topologyId, long txCreationTime) {
      super(tx, topologyId, txCreationTime);
      if (modifications != null && modifications.length != 0) {
         setModifications(Arrays.asList(modifications));
      }
   }

   public RemoteTransaction(GlobalTransaction tx, int topologyId, long txCreationTime) {
      super(tx, topologyId, txCreationTime);
   }

   @Override
   public void setStateTransferFlag(Flag stateTransferFlag) {
      if (getStateTransferFlag() == null && stateTransferFlag == Flag.PUT_FOR_X_SITE_STATE_TRANSFER) {
         internalSetStateTransferFlag(stateTransferFlag);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RemoteTransaction)) return false;
      RemoteTransaction that = (RemoteTransaction) o;
      return tx.equals(that.tx);
   }

   @Override
   public int hashCode() {
      return tx.hashCode();
   }

   @Override
   public Object clone() {
      try {
         return super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!!", e);
      }
   }

   @Override
   public String toString() {
      return "RemoteTransaction{" +
            ", lockedKeys=" + toStr(getLockedKeys()) +
            ", backupKeyLocks=" + toStr(getBackupLockedKeys()) +
            ", lookedUpEntriesTopology=" + lookedUpEntriesTopology +
            ", isMarkedForRollback=" + isMarkedForRollback() +
            ", tx=" + tx +
            ", state=" + transactionState +
            '}';
   }

   public void setLookedUpEntriesTopology(int lookedUpEntriesTopology) {
      this.lookedUpEntriesTopology = lookedUpEntriesTopology;
   }

   public int lookedUpEntriesTopology() {
      return lookedUpEntriesTopology;
   }

   void checkIfRolledBack() {
      if (isMarkedForRollback()) {
         throw new InvalidTransactionException("This remote transaction " + getGlobalTransaction() + " is already rolled back");
      }
   }

   /**
    * @return  get (or create if needed) the {@code TotalOrderRemoteTransactionState} associated to this remote transaction
    */
   public final TotalOrderRemoteTransactionState getTransactionState() {
      if (transactionState != null) {
         return transactionState;
      }
      synchronized (transactionStateLock) {
         if (transactionState == null) {
            transactionState = new TotalOrderRemoteTransactionState(getGlobalTransaction());
         }
         return transactionState;
      }
   }

   public final CompletableFuture<Void> enterSynchronizationAsync(CompletableFuture<Void> releaseFuture) {
      CompletableFuture<Void> currentFuture;
      do {
         currentFuture = synchronization.get();
      } while (!synchronization.compareAndSet(currentFuture, releaseFuture));
      return currentFuture;
   }
}
