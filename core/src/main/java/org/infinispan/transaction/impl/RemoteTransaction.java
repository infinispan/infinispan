package org.infinispan.transaction.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Defines the state of a remotely originated transaction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoteTransaction extends AbstractCacheTransaction {

   private static final Log log = LogFactory.getLog(RemoteTransaction.class);
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

   private final AtomicReference<CompletableFuture<Void>> synchronization =
         new AtomicReference<>(INITIAL_FUTURE);

   public RemoteTransaction(List<WriteCommand> modifications, GlobalTransaction tx, int topologyId, long txCreationTime) {
      super(tx, topologyId, txCreationTime);
      lookedUpEntries = new HashMap<>(modifications.size());
      setModifications(modifications);
   }

   public RemoteTransaction(GlobalTransaction tx, int topologyId, long txCreationTime) {
      super(tx, topologyId, txCreationTime);
      lookedUpEntries = new HashMap<>(4);
   }

   @Override
   public void setStateTransferFlag(Flag stateTransferFlag) {
      if (getStateTransferFlag() == null && stateTransferFlag == Flag.PUT_FOR_X_SITE_STATE_TRANSFER) {
         internalSetStateTransferFlag(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
      }
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      checkIfRolledBack();
      if (log.isTraceEnabled()) {
         log.tracef("Adding key %s to tx %s", toStr(key), getGlobalTransaction());
      }
      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> entries) {
      checkIfRolledBack();
      if (log.isTraceEnabled()) {
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
   public String toString() {
      return "RemoteTransaction{" +
            "modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
            ", lockedKeys=" + toStr(getLockedKeys()) +
            ", backupKeyLocks=" + toStr(getBackupLockedKeys()) +
            ", lookedUpEntriesTopology=" + lookedUpEntriesTopology +
            ", isMarkedForRollback=" + isMarkedForRollback() +
            ", tx=" + tx +
            '}';
   }

   public void setLookedUpEntriesTopology(int lookedUpEntriesTopology) {
      this.lookedUpEntriesTopology = lookedUpEntriesTopology;
   }

   public int lookedUpEntriesTopology() {
      return lookedUpEntriesTopology;
   }

   public final CompletableFuture<Void> enterSynchronizationAsync(CompletableFuture<Void> releaseFuture) {
      CompletableFuture<Void> currentFuture;
      do {
         currentFuture = synchronization.get();
      } while (!synchronization.compareAndSet(currentFuture, releaseFuture));
      return currentFuture;
   }
}
