package org.infinispan.transaction.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
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
      this.modifications = modifications == null || modifications.length == 0
            ? Collections.emptyList()
            : Arrays.asList(modifications);
      lookedUpEntries = new HashMap<>(this.modifications.size());
   }

   public RemoteTransaction(GlobalTransaction tx, int topologyId, long txCreationTime) {
      super(tx, topologyId, txCreationTime);
      this.modifications = new LinkedList<>();
      lookedUpEntries = new HashMap<>(4);
   }

   @Override
   public void setStateTransferFlag(Flag stateTransferFlag) {
      if (getStateTransferFlag() == null && stateTransferFlag == Flag.PUT_FOR_X_SITE_STATE_TRANSFER) {
         internalSetStateTransferFlag(stateTransferFlag);
      }
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      checkIfRolledBack();
      if (trace) {
         log.tracef("Adding key %s to tx %s", toStr(key), getGlobalTransaction());
      }
      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> entries) {
      checkIfRolledBack();
      if (trace) {
         log.tracef("Adding keys %s to tx %s", entries.keySet(), getGlobalTransaction());
      }
      lookedUpEntries.putAll(entries);
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
         RemoteTransaction dolly = (RemoteTransaction) super.clone();
         dolly.modifications = new ArrayList<>(modifications);
         dolly.lookedUpEntries = new HashMap<>(lookedUpEntries);
         return dolly;
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!!", e);
      }
   }

   @Override
   public String toString() {
      return "RemoteTransaction{" +
            "modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
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

   private void checkIfRolledBack() {
      if (isMarkedForRollback()) {
         throw new InvalidTransactionException("This remote transaction " + getGlobalTransaction() + " is already rolled back");
      }
   }

   /**
    * @return  get (or create if needed) the {@code TotalOrderRemoteTransactionState} associated to this remote transaction
    * @deprecated since 10.0. Total Order will be removed.
    */
   @Deprecated
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
