package org.infinispan.transaction.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.infinispan.commons.util.Util.toStr;

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
   protected volatile Set<Object> lockedKeys = null;

   /** Holds all the locks for which the local node is a secondary data owner. */
   protected volatile Set<Object> backupKeyLocks = null;

   private volatile boolean txComplete = false;
   protected final int topologyId;

   private EntryVersionsMap updatedEntryVersions;
   private EntryVersionsMap versionsSeenMap;

   private Map<Object, EntryVersion> lookedUpRemoteVersions;

   /** mark as volatile as this might be set from the tx thread code on view change*/
   private volatile boolean isMarkedForRollback;

   /**
    * Mark the time this tx object was created
    */
   private final long txCreationTime;

   /**
    * Equivalence function to compare keys that are stored in temporary
    * collections used in the cache transaction to keep track of locked keys,
    * looked up keys...etc.
    */
   protected final Equivalence<Object> keyEquivalence;

   private volatile Flag stateTransferFlag;

   private final CompletableFuture<Void> notifier;

   public final boolean isMarkedForRollback() {
      return isMarkedForRollback;
   }

   public void markForRollback(boolean markForRollback) {
      isMarkedForRollback = markForRollback;
   }

   public AbstractCacheTransaction(GlobalTransaction tx, int topologyId, Equivalence<Object> keyEquivalence, long txCreationTime) {
      this.tx = tx;
      this.topologyId = topologyId;
      this.keyEquivalence = keyEquivalence;
      this.txCreationTime = txCreationTime;
      notifier = new CompletableFuture<>();
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return tx;
   }

   @Override
   public final List<WriteCommand> getModifications() {
      if (hasLocalOnlyModifications) {
         return modifications.stream().filter(cmd -> !cmd.hasFlag(Flag.CACHE_MODE_LOCAL)).collect(Collectors.toList());
      } else {
         return getAllModifications();
      }
   }

   @Override
   public final List<WriteCommand> getAllModifications() {
      return modifications == null ? InfinispanCollections.<WriteCommand>emptyList() : modifications;
   }

   public final void setModifications(List<WriteCommand> modifications) {
      if (modifications == null) {
         throw new IllegalArgumentException("modification list cannot be null");
      }
      List<WriteCommand> mods = new ArrayList<>();
      for (WriteCommand cmd : modifications) {
         if (cmd.hasFlag(Flag.CACHE_MODE_LOCAL)) {
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
      if (!txComplete) {
         //avoid invalidate CPU L1 cache is tx is already completed
         txComplete = true;
         notifier.complete(null);
      }
   }

   @Override
   public final boolean waitForLockRelease(long lockAcquisitionTimeout) throws InterruptedException {
      try {
         notifier.get(lockAcquisitionTimeout, TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
         throw new IllegalStateException("Should never happen", e);
      } catch (TimeoutException e) {
         //ignored.
      }
      return txComplete;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void addBackupLockForKey(Object key) {
      // we need to synchronize this collection to be able to get a valid snapshot from another thread during state transfer
      if (backupKeyLocks == null) backupKeyLocks = Collections.synchronizedSet(new HashSet<>(INITIAL_LOCK_CAPACITY));
      backupKeyLocks.add(key);
   }

   public void registerLockedKey(Object key) {
      // we need to synchronize this collection to be able to get a valid snapshot from another thread during state transfer
      if (lockedKeys == null) lockedKeys = Collections.synchronizedSet(CollectionFactory.makeSet(INITIAL_LOCK_CAPACITY, keyEquivalence));
      if (trace) log.tracef("Registering locked key: %s", toStr(key));
      lockedKeys.add(key);
   }

   @Override
   public Set<Object> getLockedKeys() {
      return lockedKeys == null ? InfinispanCollections.emptySet() : lockedKeys;
   }

   @Override
   public Set<Object> getBackupLockedKeys() {
      return backupKeyLocks == null ?
            InfinispanCollections.emptySet() : backupKeyLocks;
   }

   @Override
   public void clearLockedKeys() {
      if (trace) log.tracef("Clearing locked keys: %s", toStr(lockedKeys));
      lockedKeys = null;
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
      return txComplete;
   }

   public Set<Object> getAffectedKeys() {
      return affectedKeys == null ? InfinispanCollections.emptySet() : affectedKeys;
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
      if (affectedKeys == null) affectedKeys = CollectionFactory.makeSet(INITIAL_LOCK_CAPACITY, keyEquivalence);
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
   public EntryVersion getLookedUpRemoteVersion(Object key) {
      return lookedUpRemoteVersions != null ? lookedUpRemoteVersions.get(key) : null;
   }

   @Override
   public void putLookedUpRemoteVersion(Object key, EntryVersion version) {
      if (lookedUpRemoteVersions == null) {
         lookedUpRemoteVersions = new HashMap<>();
      }
      lookedUpRemoteVersions.put(key, version);
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
      if (!versionsSeenMap.containsKey(key)) {
         if (log.isTraceEnabled()) {
            log.tracef("Transaction %s read %s with version %s", getGlobalTransaction().globalId(), key, version);
         }
         versionsSeenMap.put(key, (IncrementableEntryVersion) version);
      }
   }

   @Override
   public void replaceVersionRead(Object key, EntryVersion version) {
      if (version == null) {
         return;
      }
      if (versionsSeenMap == null) {
         versionsSeenMap = new EntryVersionsMap();
      }
      EntryVersion oldVersion = versionsSeenMap.put(key, (IncrementableEntryVersion) version);
      if (log.isTraceEnabled()) {
         log.tracef("Transaction %s replaced version for key %s. old=%s, new=%s", getGlobalTransaction().globalId(), key,
                    oldVersion, version);
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
      notifier.thenRun(listener::onCompletion);
   }
}
