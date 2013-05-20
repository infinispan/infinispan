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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.Flag;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.Equivalence;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.util.Util.toStr;

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

   private boolean txComplete = false;
   private volatile boolean needToNotifyWaiters = false;
   protected final int topologyId;

   private EntryVersionsMap updatedEntryVersions;

   private Map<Object, EntryVersion> lookedUpRemoteVersions;

   /** mark as volatile as this might be set from the tx thread code on view change*/
   private volatile boolean isMarkedForRollback;

   /**
    * Used internally by the {@link #waitForLockRelease} method in order to notify other transactions that wait on this
    * one to complete.
    */
   private final Object lockReleaseNotifier = new Object();

   /**
    * Equivalence function to compare keys that are stored in temporary
    * collections used in the cache transaction to keep track of locked keys,
    * looked up keys...etc.
    */
   protected final Equivalence<Object> keyEquivalence;

   public final boolean isMarkedForRollback() {
      return isMarkedForRollback;
   }

   public void markForRollback(boolean markForRollback) {
      isMarkedForRollback = markForRollback;
   }

   public AbstractCacheTransaction(GlobalTransaction tx, int topologyId, Equivalence<Object> keyEquivalence) {
      this.tx = tx;
      this.topologyId = topologyId;
      this.keyEquivalence = keyEquivalence;
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return tx;
   }

   @Override
   public final List<WriteCommand> getModifications() {
      if (hasLocalOnlyModifications) {
         List<WriteCommand> mods = new ArrayList<WriteCommand>();
         for (WriteCommand cmd : modifications) {
            if (!cmd.hasFlag(Flag.CACHE_MODE_LOCAL)) {
               mods.add(cmd);
            }
         }
         return mods;
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
      List<WriteCommand> mods = new ArrayList<WriteCommand>();
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
      txComplete = true; //this one is cheap but does not guarantee visibility
      if (needToNotifyWaiters) {
         synchronized (lockReleaseNotifier) {
            txComplete = true; //in this case we want to guarantee visibility to other threads
            lockReleaseNotifier.notifyAll();
         }
      }
   }

   @Override
   public boolean waitForLockRelease(Object key, long lockAcquisitionTimeout) throws InterruptedException {
      if (txComplete) return true; //using an unsafe optimisation: if it's true, we for sure have the latest read of the value without needing memory barriers
      final boolean potentiallyLocked = hasLockOrIsLockBackup(key);
      if (trace) log.tracef("Transaction gtx=%s potentially locks key %s? %s", tx, key, potentiallyLocked);
      if (potentiallyLocked) {
         synchronized (lockReleaseNotifier) {
            // Check again after acquiring a lock on the monitor that the transaction has completed.
            // If it has completed, all of its locks would have been released.
            needToNotifyWaiters = true;
            //The order in which these booleans are verified is critical as we take advantage of it to avoid otherwise needed locking
            if (txComplete) {
               needToNotifyWaiters = false;
               return true;
            }
            lockReleaseNotifier.wait(lockAcquisitionTimeout);

            // Check again in case of spurious thread signalling
            return txComplete;
         }
      }
      return true;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void addBackupLockForKey(Object key) {
      // we need to synchronize this collection to be able to get a valid snapshot from another thread during state transfer
      if (backupKeyLocks == null) backupKeyLocks = Collections.synchronizedSet(new HashSet<Object>(INITIAL_LOCK_CAPACITY));
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

   private boolean hasLockOrIsLockBackup(Object key) {
      //stopgap fix for ISPN-2728. The real fix would be to synchronize this with the intrinsic lock.
      Set<Object> lockedKeysCopy = lockedKeys;
      Set<Object> backupKeyLocksCopy = backupKeyLocks;
      return (lockedKeysCopy != null && lockedKeysCopy.contains(key)) || (backupKeyLocksCopy != null && backupKeyLocksCopy.contains(key));
   }

   public Set<Object> getAffectedKeys() {
      return affectedKeys == null ? InfinispanCollections.emptySet() : affectedKeys;
   }

   public void addAffectedKey(Object key) {
      initAffectedKeys();
      affectedKeys.add(key);
   }

   public void addAllAffectedKeys(Collection<Object> keys) {
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
         lookedUpRemoteVersions = new HashMap<Object, EntryVersion>();
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
}
