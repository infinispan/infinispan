package org.infinispan.transaction.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.commands.write.WriteCommand;
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
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

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
   private static final Log log = LogFactory.getLog(AbstractCacheTransaction.class);
   private static final boolean trace = log.isTraceEnabled();

   volatile boolean hasLocalOnlyModifications;
   protected volatile List<WriteCommand> modifications;

   protected final Map<Object, ContextEntry> entries = new ConcurrentHashMap<>();
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
   }

   @Override
   public final GlobalTransaction getGlobalTransaction() {
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
         return Collections.emptyList();
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

   @Override
   public void freezeModifications() {
      if (modifications != null) {
         modifications = Immutables.immutableListCopy(modifications);
      } else {
         modifications = Collections.emptyList();
      }
   }

   @Override
   public final void putLookedUpEntry(Object key, CacheEntry e) {
      checkIfRolledBack();
      entries.computeIfAbsent(key, ContextEntry::new).cacheEntry = e;
   }

   @Override
   public final Map<Object, CacheEntry> getLookedUpEntries() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final CacheEntry lookupEntry(Object key) {
      if (key == null) {
         return null; //org.infinispan.container.entries.NullCacheEntry has null key
      }
      ContextEntry entry = entries.get(key);
      return entry == null ? null : entry.cacheEntry;
   }

   @Override
   public final void clearLookedUpEntries() {
      for (ContextEntry entry : entries.values()) {
         entry.cacheEntry = null;
      }
   }

   @Override
   public final boolean ownsLock(Object key) {
      ContextEntry entry = entries.get(key);
      return entry != null && entry.isLocked();
   }

   @Override
   public final void removeLookedUpEntry(Object key) {
      ContextEntry entry = entries.get(key);
      if (entry != null) {
         entry.cacheEntry = null;
      }
   }

   @Override
   public void notifyOnTransactionFinished() {
      //TODO!
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
   public final int getTopologyId() {
      return topologyId;
   }

   @Override
   public final void addBackupLockForKey(Object key) {
      entries.computeIfAbsent(key, ContextEntry::new).addBackupLock();
   }

   public final void registerLockedKey(Object key) {
      if (trace) log.tracef("Registering locked key: %s", toStr(key));
      entries.computeIfAbsent(key, ContextEntry::new).addLock();
   }

   @Override
   public final Set<Object> getLockedKeys() {
      return entries.values().stream().filter(ContextEntry::isLocked).map(ContextEntry::getKey).collect(Collectors.toSet());
   }

   @Override
   public final Set<Object> getBackupLockedKeys() {
      return entries.values().stream().filter(ContextEntry::isBackupLocked).map(ContextEntry::getKey).collect(Collectors.toSet());
   }

   @Override
   public void clearLockedKeys() {
      if (trace) {
         log.tracef("Clearing locked keys:");
      }
      entries.values().forEach(ContextEntry::releaseLock);
   }

   @Override
   public boolean containsLockOrBackupLock(Object key) {
      ContextEntry entry = entries.get(key);
      return entry != null && entry.isLockedOrBackupLocked();
   }

   @Override
   public Object findAnyLockedOrBackupLocked(Collection<Object> keys) {
      for (Object key : keys) {
         if (containsLockOrBackupLock(key)) {
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
      return entries.values().stream()
            .filter(contextEntry -> contextEntry.clusterWideLocked)
            .map(contextEntry -> contextEntry.key)
            .collect(Collectors.toSet());
   }

   public void addAffectedKey(Object key) {
      entries.computeIfAbsent(key, ContextEntry::new).clusterWideLocked = true;
   }

   public void addAllAffectedKeys(Collection<?> keys) {
      for (Object key : keys) {
         entries.computeIfAbsent(key, ContextEntry::new).clusterWideLocked = true;
      }
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
   public void addVersionRead(Object key, EntryVersion version) {
      if (version == null) {
         return;
      }
      if (versionsSeenMap == null) {
         versionsSeenMap = new EntryVersionsMap();
      }
      if (!versionsSeenMap.containsKey(key)) {
         if (trace) {
            log.tracef("Transaction %s read %s with version %s", getGlobalTransaction().globalId(), key, version);
         }
         versionsSeenMap.put(key, (IncrementableEntryVersion) version);
      }
   }

   @Override
   public final EntryVersionsMap getVersionsRead() {
      return versionsSeenMap == null ? new EntryVersionsMap() : versionsSeenMap;
   }

   public final boolean isFromStateTransfer() {
      return stateTransferFlag != null;
   }

   public final Flag getStateTransferFlag() {
      return stateTransferFlag;
   }

   public abstract void setStateTransferFlag(Flag stateTransferFlag);

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
      ContextEntry entry = entries.get(key);
      if (entry == null || entry.isNotLockedOrBackupLock()) {
         return null;
      } else if (entry.isLocked()) {
         return txCompleted;
      } else {
         return entry.getBackupLockCF();
      }
   }

   @Override
   public Map<Object, CompletableFuture<Void>> getReleaseFutureForKeys(Collection<Object> keys) {
      Map<Object, CompletableFuture<Void>> result = null;
      for (Object key : keys) {
         ContextEntry entry = entries.get(key);
         if (entry == null) {
            continue;
         }
         if (entry.isLocked()) {
            return Collections.singletonMap(key, txCompleted);
         } else {
            CompletableFuture<Void> cf = entry.getBackupLockCF();
            if (cf != null) {
               if (result == null) {
                  result = new HashMap<>();
               }
               result.put(key, cf);
            }
         }
      }
      return result == null ? Collections.emptyMap() : result;
   }

   @Override
   public void cleanupBackupLocks() {
      entries.values().forEach(ContextEntry::releaseBackupLock);
   }

   @Override
   public synchronized void removeBackupLocks(Collection<?> keys) {
      keys.forEach(this::removeBackupLock);
   }

   public synchronized void removeBackupLock(Object key) {
      ContextEntry entry = entries.get(key);
      if (entry != null && entry.isBackupLocked()) {
         entry.releaseBackupLock();
      }
   }

   @Override
   public synchronized void forEachBackupLock(Consumer<Object> consumer) {
      entries.values().stream().filter(ContextEntry::isBackupLocked).map(ContextEntry::getKey).forEach(consumer);
   }

   final void internalSetStateTransferFlag(Flag stateTransferFlag) {
      this.stateTransferFlag = stateTransferFlag;
   }

   @Override
   public final EntryVersion getLookedUpRemoteVersion(Object key) {
      return null;
   }

   @Override
   public final void replaceVersionRead(Object key, EntryVersion version) {
   }

   @Override
   public final boolean hasModification(Class<?> modificationClass) {
      return false;
   }

   @Override
   public final void putLookedUpEntries(Map<Object, CacheEntry> entries) {
   }

   @Override
   public final void putLookedUpRemoteVersion(Object key, EntryVersion version) {
   }


   @Override
   public final void addReadKey(Object key) {
   }

   @Override
   public final boolean keyRead(Object key) {
      return false;
   }

   public void forEachValue(BiConsumer<Object, CacheEntry> action) {
      for (ContextEntry entry : entries.values()) {
         if (entry.cacheEntry == null || entry.cacheEntry.isRemoved() || entry.cacheEntry.isNull()) {
            continue;
         }
         action.accept(entry.key, entry.cacheEntry);
      }
   }

   public int lookedUpEntriesCount() {
      int size = 0;
      for (ContextEntry entry : entries.values()) {
         if (entry.cacheEntry != null) {
            size++;
         }
      }
      return size;
   }

   public void forEachEntry(BiConsumer<Object, CacheEntry> action) {
      for (ContextEntry entry : entries.values()) {
         if (entry.cacheEntry == null) {
            continue;
         }
         action.accept(entry.key, entry.cacheEntry);
      }
   }

   public boolean hasLookupEntries() {
      return entries.values().stream().anyMatch(contextEntry -> contextEntry.cacheEntry != null);
   }

   @Override
   public void forEachLock(Consumer<Object> consumer) {
      entries.values().stream().filter(ContextEntry::isLocked).map(ContextEntry::getKey).forEach(consumer);
   }

   abstract void checkIfRolledBack();

   private enum LockMode {
      PRIMARY,
      BACKUP
   }

   private static class ContextEntry implements Map.Entry<Object, CacheEntry> {
      private final Object key;
      private CacheEntry cacheEntry; //entry read/written
      private boolean clusterWideLocked; //true if the key is locked somewhere in the cluster
      @GuardedBy("this")
      private LockMode lockMode; //lock mode. null for not locked
      @GuardedBy("this")
      private CompletableFuture<Void> backupLockFuture;

      private ContextEntry(Object key) {
         this.key = key;
      }

      @Override
      public Object getKey() {
         return key;
      }

      @Override
      public CacheEntry getValue() {
         return cacheEntry;
      }

      @Override
      public CacheEntry setValue(CacheEntry value) {
         throw new UnsupportedOperationException();
      }

      synchronized CompletableFuture<Void> getBackupLockCF() {
         return backupLockFuture;
      }

      synchronized boolean isLockedOrBackupLocked() {
         return lockMode != null;
      }

      synchronized boolean isNotLockedOrBackupLock() {
         return lockMode == null;
      }

      synchronized void addLock() {
         lockMode = LockMode.PRIMARY;
      }

      synchronized boolean isLocked() {
         return lockMode == LockMode.PRIMARY;
      }

      synchronized boolean isBackupLocked() {
         return lockMode == LockMode.BACKUP;
      }

      synchronized void releaseLock() {
         if (lockMode == LockMode.PRIMARY) {
            lockMode = null;
         }
      }

      synchronized void releaseBackupLock() {
         if (lockMode == LockMode.BACKUP) {
            lockMode = null;
            backupLockFuture.complete(null);
            backupLockFuture = null;
         }
      }

      synchronized void addBackupLock() {
         lockMode = LockMode.BACKUP;
         backupLockFuture = new CompletableFuture<>();
      }
   }

}
