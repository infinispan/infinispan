/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.concurrent.locks.StripedLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This class extends {@link AbstractCacheStore} adding lock support for consistently accessing stored data.
 * <p/>
 * In-memory locking is needed by aggregation operations(e.g. loadAll, toStream, fromStream) to make sure that
 * manipulated data won't be corrupted by concurrent access to Store. It also assures atomic data access for each stored
 * entry.
 * <p/>
 * Locking is based on a {@link StripedLock}. You can tune the concurrency level of the striped lock (see the Javadocs
 * of StripedLock for details on what this is) by using the {@link LockSupportCacheStoreConfig#setLockConcurrencyLevel(int)}
 * setter.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @param <L> the type of the locking key returned by
 *            {@link #getLockFromKey(Object)}
 */
public abstract class LockSupportCacheStore<L> extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(LockSupportCacheStore.class);
   private static final boolean trace = log.isTraceEnabled();

   private StripedLock locks;
   private long globalLockTimeoutMillis;
   private LockSupportCacheStoreConfig config;

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (LockSupportCacheStoreConfig) config;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      if (config == null) {
        throw new CacheLoaderException("Null config. Possible reason is not calling super.init(...)");
      }
      log.tracef("Starting cache with config: %s", config);

      locks = new StripedLock(config.getLockConcurrencyLevel());
      globalLockTimeoutMillis = config.getLockAcquistionTimeout();
   }

   /**
    * Release the locks (either read or write).
    */
   protected final void unlock(L key) {
      locks.releaseLock(key);
   }

   /**
    * Acquires write lock on the given key.
    */
   protected final void lockForWriting(L key) {
      locks.acquireLock(key, true);
   }

   /**
    * Acquires read lock on the given key.
    */
   protected final void lockForReading(L key) {
      locks.acquireLock(key, false);
   }

   /**
    * Upgrades a read lock to a write lock.
    */
   protected final void upgradeLock(L key) {
      locks.upgradeLock(key);
   }

   /**
    * Downgrade a write lock to a read lock
    */
   protected final void downgradeLock(L key) {
      locks.downgradeLock(key);
   }

   /**
    * Same as {@link #lockForWriting(Object)}, but with 0 timeout.
    */
   protected final boolean immediateLockForWriting(L key) {
      return locks.acquireLock(key, true, 0);
   }

   /**
    * Based on the supplied param, acquires a global read(false) or write (true) lock.
    */
   protected final boolean acquireGlobalLock(boolean exclusive) {
      return locks.acquireGlobalLock(exclusive, globalLockTimeoutMillis);
   }

   /**
    * Based on the supplied param, releases a global read(false) or write (true) lock.
    */
   protected final void releaseGlobalLock(boolean exclusive) {
      locks.releaseGlobalLock(exclusive);
   }

   @Override
   public final InternalCacheEntry load(Object key) throws CacheLoaderException {
      L lockingKey = getLockFromKey(key);
      lockForReading(lockingKey);
      try {
         return loadLockSafe(key, lockingKey);
      } finally {
         unlock(lockingKey);
      }
   }

   @Override
   public final Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      boolean success = acquireGlobalLock(false);
      try {
         return loadAllLockSafe();
      } finally {
         if(success){
            releaseGlobalLock(false);
         }
      }
   }

   @Override
   public final Set<InternalCacheEntry> load(int maxEntries) throws CacheLoaderException {
      if (maxEntries < 0) {
         return loadAll();
      }
      boolean success = acquireGlobalLock(false);
      try {
         return loadLockSafe(maxEntries);
      } finally {
         if(success){
            releaseGlobalLock(false);
         }
      }
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      boolean success = acquireGlobalLock(false);
      try {
         return loadAllKeysLockSafe(keysToExclude);
      } finally {
         if(success){
            releaseGlobalLock(false);
         }
      }
   }


   @Override
   public final void store(InternalCacheEntry ed) throws CacheLoaderException {
      if (trace) {
         log.tracef("store(%s)", ed);
      }
      if (ed == null) {
        return;
      }
      if (ed.canExpire() && ed.isExpired(timeService.wallClockTime())) {
         if (containsKey(ed.getKey())) {
            if (trace) {
               log.tracef("Entry %s is expired!  Removing!", ed);
            }
            remove(ed.getKey());
         } else {
            if (trace) {
               log.tracef("Entry %s is expired!  Not doing anything.", ed);
            }
         }
         return;
      }

      L keyHashCode = getLockFromKey(ed.getKey());
      lockForWriting(keyHashCode);
      try {
         storeLockSafe(ed, keyHashCode);
      } finally {
         unlock(keyHashCode);
      }
      if (trace) {
         log.tracef("exit store(%s)", ed);
      }
   }

   @Override
   public final boolean remove(Object key) throws CacheLoaderException {
      if (trace) {
         log.tracef("remove(%s)", key);
      }
      L keyHashCode = getLockFromKey(key);
      try {
         lockForWriting(keyHashCode);
         return removeLockSafe(key, keyHashCode);
      } finally {
         unlock(keyHashCode);
         if (trace) {
            log.tracef("Exit remove(%s)", key);
         }
      }
   }

   @Override
   public final void fromStream(ObjectInput objectInput) throws CacheLoaderException {
      boolean success = acquireGlobalLock(true);
      try {
         fromStreamLockSafe(objectInput);
      } finally {
         if(success){
            releaseGlobalLock(true);
         }
      }
   }

   @Override
   public void toStream(ObjectOutput objectOutput) throws CacheLoaderException {
      boolean success = acquireGlobalLock(false);
      try {
         toStreamLockSafe(objectOutput);
      } finally {
         if(success){
            releaseGlobalLock(false);
         }
      }
   }

   @Override
   public final void clear() throws CacheLoaderException {
      if (trace) {
         log.trace("Clearing store");
      }
      boolean success = acquireGlobalLock(true);
      try {
         clearLockSafe();
      } finally {
         if(success){
            releaseGlobalLock(true);
         }
      }
   }

   public int getTotalLockCount() {
      return locks.getTotalLockCount();
   }

   protected abstract void clearLockSafe() throws CacheLoaderException;

   protected abstract Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException;

   protected abstract Set<InternalCacheEntry> loadLockSafe(int maxEntries) throws CacheLoaderException;

   protected abstract Set<Object> loadAllKeysLockSafe(Set<Object> keysToExclude) throws CacheLoaderException;

   protected abstract void toStreamLockSafe(ObjectOutput oos) throws CacheLoaderException;

   protected abstract void fromStreamLockSafe(ObjectInput ois) throws CacheLoaderException;

   protected abstract boolean removeLockSafe(Object key, L lockingKey) throws CacheLoaderException;

   protected abstract void storeLockSafe(InternalCacheEntry ed, L lockingKey) throws CacheLoaderException;

   protected abstract InternalCacheEntry loadLockSafe(Object key, L lockingKey) throws CacheLoaderException;

   protected abstract L getLockFromKey(Object key) throws CacheLoaderException;
}
