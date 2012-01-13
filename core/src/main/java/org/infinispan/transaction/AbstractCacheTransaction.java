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

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
   private static final int INITIAL_LOCK_CAPACITY = 4;

   protected List<WriteCommand> modifications;
   protected HashMap<Object, CacheEntry> lookedUpEntries;
   protected Set<Object> affectedKeys = null;
   protected Set<Object> lockedKeys;
   protected Set<Object> backupKeyLocks = null;
   private boolean txComplete = false;
   protected volatile boolean prepared;
   final int viewId;

   private EntryVersionsMap updatedEntryVersions;

   public AbstractCacheTransaction(GlobalTransaction tx, int viewId) {
      this.tx = tx;
      this.viewId = viewId;
   }

   public GlobalTransaction getGlobalTransaction() {
      return tx;
   }

   public List<WriteCommand> getModifications() {
      return modifications;
   }

   public void setModifications(WriteCommand[] modifications) {
      this.modifications = Arrays.asList(modifications);
   }

   public Map<Object, CacheEntry> getLookedUpEntries() {
      return lookedUpEntries;
   }

   public CacheEntry lookupEntry(Object key) {
      if (lookedUpEntries == null) return null;
      return lookedUpEntries.get(key);
   }

   public void removeLookedUpEntry(Object key) {
      if (lookedUpEntries != null) lookedUpEntries.remove(key);
   }

   public void clearLookedUpEntries() {
      lookedUpEntries = null;
   }

   @Override
   public boolean ownsLock(Object key) {
      return getLockedKeys().contains(key);
   }

   @Override
   public void notifyOnTransactionFinished() {
      log.tracef("Transaction %s has completed, notifying listening threads.", tx);
      synchronized (this) {
         txComplete = true;
         this.notifyAll();
      }
   }

   @Override
   public boolean waitForLockRelease(Object key, long lockAcquisitionTimeout) throws InterruptedException {
      final boolean potentiallyLocked = hasLockOrIsLockBackup(key);
      log.tracef("Transaction gtx=%s potentially locks key %s? %s", tx, key, potentiallyLocked);
      if (potentiallyLocked) {
         synchronized (this) {
            this.wait(lockAcquisitionTimeout);
            return txComplete;
         }
      }
      return true;
   }

   @Override
   public int getViewId() {
      return viewId;
   }

   @Override
   public void addBackupLockForKey(Object key) {
      if (backupKeyLocks == null) backupKeyLocks = new HashSet<Object>(INITIAL_LOCK_CAPACITY);
      backupKeyLocks.add(key);
   }

   public void registerLockedKey(Object key) {
      if (lockedKeys == null) lockedKeys = new HashSet<Object>(INITIAL_LOCK_CAPACITY);
      log.tracef("Registering locked key: %s", key);
      lockedKeys.add(key);
   }

   public Set<Object> getLockedKeys() {
      return lockedKeys == null ? Collections.emptySet() : lockedKeys;
   }

   public void clearLockedKeys() {
      log.tracef("Clearing locked keys: %s", lockedKeys);
      lockedKeys = null;
   }

   private boolean hasLockOrIsLockBackup(Object key) {
      return (lockedKeys != null && lockedKeys.contains(key)) || (backupKeyLocks != null && backupKeyLocks.contains(key));
   }

   public Set<Object> getAffectedKeys() {
      return affectedKeys == null ? Collections.emptySet() : affectedKeys;
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
      if (affectedKeys == null) affectedKeys = new HashSet<Object>(INITIAL_LOCK_CAPACITY);
   }

   @Override
   public EntryVersionsMap getUpdatedEntryVersions() {
      return updatedEntryVersions;
   }

   @Override
   public void setUpdatedEntryVersions(EntryVersionsMap updatedEntryVersions) {
      this.updatedEntryVersions = updatedEntryVersions;
   }
}
