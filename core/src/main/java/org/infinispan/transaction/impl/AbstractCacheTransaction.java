package org.infinispan.transaction.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.ImmutableListCopy;
import org.infinispan.commons.util.Immutables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Base class for local and remote transaction. Impl note: The aggregated modification list and lookedUpEntries are not
 * instantiated here but in subclasses. This is done in order to take advantage of the fact that, for remote
 * transactions we already know the size of the modifications list at creation time.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.2
 */
public abstract class AbstractCacheTransaction implements CacheTransaction {

   protected final GlobalTransaction tx;
   private static Log log = LogFactory.getLog(AbstractCacheTransaction.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final int INITIAL_LOCK_CAPACITY = 4;

   protected volatile boolean hasLocalOnlyModifications;
   protected volatile List<WriteCommand> modifications;

   protected Map<Object, CacheEntry> lookedUpEntries;

   /** Holds all the locked keys that were acquired by the transaction allover the cluster. */
   protected Set<Object> affectedKeys = null;

   /** Holds all the keys that were actually locked on the local node. */
   private final AtomicReference<Set<Object>> lockedKeys = new AtomicReference<>();

   /** Holds all the locks for which the local node is a secondary data owner. */
   private final AtomicReference<Set<Object>> backupKeyLocks = new AtomicReference<>();

   protected final int topologyId;

   private EntryVersionsMap updatedEntryVersions;
   private EntryVersionsMap versionsSeenMap;

   /** mark as volatile as this might be set from the tx thread code on view change*/
   private volatile boolean isMarkedForRollback;

   /**
    * Mark the time this tx object was created
    */
   private final long txCreationTime;

   private volatile Flag stateTransferFlag;

   private final CompletableFuture<Void> txCompleted;
   private volatile CompletableFuture<Void> backupLockReleased;

   public final boolean isMarkedForRollback() {
      return isMarkedForRollback;
   }

   public void markForRollback(boolean markForRollback) {
      isMarkedForRollback = markForRollback;
   }

   public AbstractCacheTransaction(GlobalTransaction tx, int topologyId, long txCreationTime) {
      this.tx = tx;
      this.topologyId = topologyId;
      this.txCreationTime = txCreationTime;
      txCompleted = new CompletableFuture<>();
      backupLockReleased = new CompletableFuture<>();
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return tx;
   }

   @Override
   public final List<WriteCommand> getModifications() {
      if (hasLocalOnlyModifications) {
         return modifications.stream().filter(cmd -> !cmd.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)).collect(Collectors.toList());
      } else {
         return getAllModifications();
      }
   }

   @Override
   public final List<WriteCommand> getAllModifications() {
      if (modifications instanceof ImmutableListCopy)
         return modifications;
      else if (modifications == null)
         return Collections.<WriteCommand>emptyList();
      else
         return Immutables.immutableListCopy(modifications);
   }

   public final void setModifications(List<WriteCommand> modifications) {
      if (modifications == null) {
         throw new IllegalArgumentException("modification list cannot be null");
      }
      List<WriteCommand> mods = new ArrayList<>(modifications.size());
      for (WriteCommand cmd : modifications) {
         if (cmd.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
            hasLocalOnlyModifications = true;
         }
         mods.add(cmd);
      }
      // we need to synchronize this collection to be able to get a valid copy from another thread during state transfer
      this.modifications = Collections.synchronizedList(mods);
   }

   public final boolean hasModification(Class<?> modificationClass) {
      if (modifications != null) {
         for (WriteCommand mod : getModifications()) {
            if (modificationClass.isAssignableFrom(mod.getClass())) {
               return true;
            }
         }
      }
      return false;
   }


   @Override
   public void freezeModifications() {
      if (modifications != null) {
         modifications = Immutables.immutableListCopy(modifications);
      } else {
         modifications = Collections.emptyList();
      }
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return lookedUpEntries;
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      if (lookedUpEntries == null) return null;
      return lookedUpEntries.get(key);
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      if (lookedUpEntries != null) lookedUpEntries.remove(key);
   }

   @Override
   public void clearLookedUpEntries() {
      lookedUpEntries = null;
   }

   @Override
   public boolean ownsLock(Object key) {
      return getLockedKeys().contains(key);
   }

   @Override
   public void notifyOnTransactionFinished() {
      if (trace) log.tracef("Transaction %s has completed, notifying listening threads.", tx);
      if (!txCompleted.isDone()) {
         txCompleted.complete(null);
         cleanupBackupLocks();
      }
   }

   @Override
   public final boolean waitForLockRelease(long lockAcquisitionTimeout) throws InterruptedException {
      return CompletableFutures.await(txCompleted, lockAcquisitionTimeout, TimeUnit.MILLISECONDS);
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void addBackupLockForKey(Object key) {
      // we need a synchronized collection to be able to get a valid snapshot from another thread during state transfer
      final Set<Object> keys = backupKeyLocks.updateAndGet((value) -> value == null ? Collections.synchronizedSet(new HashSet<>(INITIAL_LOCK_CAPACITY)) : value);
      keys.add(key);
   }

   public void registerLockedKey(Object key) {
      // we need a synchronized collection to be able to get a valid snapshot from another thread during state transfer
      final Set<Object> keys = lockedKeys.updateAndGet((value) -> value == null ? Collections.synchronizedSet(CollectionFactory.makeSet(INITIAL_LOCK_CAPACITY)) : value);
      if (trace) log.tracef("Registering locked key: %s", toStr(key));
      keys.add(key);
   }

   @Override
   public Set<Object> getLockedKeys() {
      final Set<Object> keys = lockedKeys.get();
      return keys == null ? Collections.emptySet() : keys;
   }

   @Override
   public Set<Object> getBackupLockedKeys() {
      final Set<Object> keys = backupKeyLocks.get();
      return keys == null ? Collections.emptySet() : keys;
   }

   @Override
   public void clearLockedKeys() {
      if (trace) log.tracef("Clearing locked keys: %s", toStr(lockedKeys.get()));
      lockedKeys.set(null);
   }

   @Override
   public boolean containsLockOrBackupLock(Object key) {
      return getLockedKeys().contains(key) || getBackupLockedKeys().contains(key);
   }

   @Override
   public Object findAnyLockedOrBackupLocked(Collection<Object> keys) {
      Set<Object> lockedKeysCopy = getLockedKeys();
      Set<Object> backupKeyLocksCopy = getBackupLockedKeys();
      for (Object key : keys) {
         if (lockedKeysCopy.contains(key) || backupKeyLocksCopy.contains(key)) {
            return key;
         }
      }
      return null;
   }

   @Override
   public boolean areLocksReleased() {
      return txCompleted.isDone();
   }

   public Set<Object> getAffectedKeys() {
      return affectedKeys == null ? Collections.emptySet() : affectedKeys;
   }

   public void addAffectedKey(Object key) {
      initAffectedKeys();
      affectedKeys.add(key);
   }

   public void addAllAffectedKeys(Collection<?> keys) {
      initAffectedKeys();
      affectedKeys.addAll(keys);
   }

   private void initAffectedKeys() {
      if (affectedKeys == null) affectedKeys = CollectionFactory.makeSet(INITIAL_LOCK_CAPACITY);
   }

   @Override
   public EntryVersionsMap getUpdatedEntryVersions() {
      return updatedEntryVersions;
   }

   @Override
   public void setUpdatedEntryVersions(EntryVersionsMap updatedEntryVersions) {
      this.updatedEntryVersions = updatedEntryVersions;
   }

   @Override
   public void addReadKey(Object key) {
      // No-op
   }

   @Override
   public boolean keyRead(Object key) {
      return false;
   }

   @Override
   public void addVersionRead(Object key, EntryVersion version) {
      if (version == null) {
         return;
      }
      if (versionsSeenMap == null) {
         versionsSeenMap = new EntryVersionsMap();
      }
      Object prevVersion = versionsSeenMap.putIfAbsent(key, (IncrementableEntryVersion) version);
      if (trace) {
         if (prevVersion == null) {
            log.tracef("Transaction %s read %s with version %s", getGlobalTransaction().globalId(), key, version);
         } else {
            log.tracef("Ignoring version read %s as already contains %s", version, prevVersion);
         }
      }
   }

   @Override
   public EntryVersionsMap getVersionsRead() {
      return versionsSeenMap == null ? new EntryVersionsMap() : versionsSeenMap;
   }

   public final boolean isFromStateTransfer() {
      return stateTransferFlag != null;
   }

   public final Flag getStateTransferFlag() {
      return stateTransferFlag;
   }

   public abstract void setStateTransferFlag(Flag stateTransferFlag);

   protected final void internalSetStateTransferFlag(Flag stateTransferFlag) {
      this.stateTransferFlag = stateTransferFlag;
   }

   @Override
   public long getCreationTime() {
      return txCreationTime;
   }

   @Override
   public final void addListener(TransactionCompletedListener listener) {
      txCompleted.thenRun(listener::onCompletion);
   }

   @Override
   public CompletableFuture<Void> getReleaseFutureForKey(Object key) {
      if (getLockedKeys().contains(key)) {
         return txCompleted;
      } else if (getBackupLockedKeys().contains(key)) {
         return backupLockReleased;
      }
      return null;
   }

   @Override
   public KeyValuePair<Object, CompletableFuture<Void>> getReleaseFutureForKeys(Collection<Object> keys) {
      Set<Object> locked = getLockedKeys();
      Set<Object> backupLocked = getBackupLockedKeys();
      Object backupKey = null;
      for (Object key : keys) {
         if (locked.contains(key)) {
            return new KeyValuePair<>(key, txCompleted);
         } else if (backupLocked.contains(key)) {
            backupKey = key;
         }
      }
      return backupKey == null ? null : new KeyValuePair<>(backupKey, backupLockReleased);
   }

   @Override
   public void cleanupBackupLocks() {
      backupKeyLocks.getAndUpdate((value) -> {
         if (value != null) {
            backupLockReleased.complete(null);
            backupLockReleased = new CompletableFuture<>();
            value.clear();
         }
         return value;
      });
   }
}
