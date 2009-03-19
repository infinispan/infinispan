package org.horizon.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.horizon.Cache;
import org.horizon.lock.StripedLock;
import org.horizon.marshall.Marshaller;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * This class extends {@link org.horizon.loader.AbstractCacheStore} adding lock support for consistently acceessing
 * stored data.
 * <p/>
 * In-memory locking is needed by aggregation operations(e.g. loadAll, toStream, fromStream) to make sure that
 * manipulated data won't be corrupted by concurrent access to Store. It also assurce atomic data access for each stored
 * entry.
 * <p/>
 * Locking is based on a {@link org.horizon.lock.StripedLock}. You can tune the concurrency level of the striped lock
 * (see the Javadocs of StripedLock for details on what this is) by using the {@link
 * org.horizon.loader.LockSupportCacheStore#setLockConcurrencyLevel(int)} setter.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class LockSupportCacheStore extends AbstractCacheStore {

   private static Log log = LogFactory.getLog(LockSupportCacheStore.class);

   private StripedLock locks;
   private long globalLockTimeoutMillis;
   private LockSupportCacheStoreConfig config;

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) {
      super.init(config, cache, m);
      this.config = (LockSupportCacheStoreConfig) config;
   }

   public void start() throws CacheLoaderException {
      super.start();
      if (config == null)
         throw new CacheLoaderException("Null config. Possible reason is not calling super.init(...)");
      locks = new StripedLock(config.getLockConcurrencyLevel());
      globalLockTimeoutMillis = config.getLockAcquistionTimeout();
   }

   /**
    * Release the locks (either read or write).
    */
   protected void unlock(String key) {
      locks.releaseLock(key);
   }

   /**
    * Acquires write lock on the given key.
    */
   protected void lockForWritting(String key) throws CacheLoaderException {
      locks.acquireLock(key, true);
   }

   /**
    * Acquires read lock on the given key.
    */
   protected void lockForReading(String key) throws CacheLoaderException {
      locks.acquireLock(key, false);
   }

   /**
    * Same as {@link #lockForWritting(String)}, but with 0 timeout.
    */
   protected boolean immediateLockForWritting(String key) throws CacheLoaderException {
      return locks.acquireLock(key, true, 0);
   }

   /**
    * Based on the supplied param, acquires a global read(false) or write (true) lock.
    */
   protected void acquireGlobalLock(boolean exclusive) throws CacheLoaderException {
      locks.aquireGlobalLock(exclusive, globalLockTimeoutMillis);
   }

   /**
    * Based on the supplied param, releases a global read(false) or write (true) lock.
    */
   protected void releaseGlobalLock(boolean exclusive) {
      locks.releaseGlobalLock(exclusive);
   }

   public final StoredEntry load(Object key) throws CacheLoaderException {
      if (log.isTraceEnabled()) log.trace("load (" + key + ")");
      String lockingKey = getLockFromKey(key);
      lockForReading(lockingKey);
      try {
         return loadLockSafe(key, lockingKey);
      } finally {
         unlock(lockingKey);
         if (log.isTraceEnabled()) log.trace("Exit load (" + key + ")");
      }
   }

   public final Set<StoredEntry> loadAll() throws CacheLoaderException {
      if (log.isTraceEnabled()) log.trace("loadAll()");
      acquireGlobalLock(false);
      try {
         return loadAllLockSafe();
      } finally {
         releaseGlobalLock(false);
         if (log.isTraceEnabled()) log.trace("Exit loadAll()");
      }
   }

   public final void store(StoredEntry ed) throws CacheLoaderException {
      if (log.isTraceEnabled()) log.trace("store(" + ed + ")");
      if (ed == null) return;
      if (ed.isExpired()) {
         log.trace("Entry " + ed + " is expired!  Not doing anything.");
         return;
      }

      String keyHashCode = getLockFromKey(ed.getKey());
      lockForWritting(keyHashCode);
      try {
         storeLockSafe(ed, keyHashCode);
      } finally {
         unlock(keyHashCode);
      }
      if (log.isTraceEnabled()) log.trace("exit store(" + ed + ")");
   }

   public final boolean remove(Object key) throws CacheLoaderException {
      if (log.isTraceEnabled()) log.trace("remove(" + key + ")");
      String keyHashCodeStr = getLockFromKey(key);
      try {
         lockForWritting(keyHashCodeStr);
         return removeLockSafe(key, keyHashCodeStr);
      } finally {
         unlock(keyHashCodeStr);
         if (log.isTraceEnabled()) log.trace("Exit remove(" + key + ")");
      }
   }

   public final void fromStream(ObjectInput objectInput) throws CacheLoaderException {
      try {
         acquireGlobalLock(true);
         // first clear all local state
         clear();
         fromStreamLockSafe(objectInput);
      } finally {
         releaseGlobalLock(true);
      }
   }

   public void toStream(ObjectOutput objectOutput) throws CacheLoaderException {
      try {
         acquireGlobalLock(false);
         toStreamLockSafe(objectOutput);
      } finally {
         releaseGlobalLock(false);
      }
   }

   public final void clear() throws CacheLoaderException {
      log.trace("Clearing store");
      try {
         acquireGlobalLock(true);
         clearLockSafe();
      } finally {
         releaseGlobalLock(true);
      }
   }

   public int getTotalLockCount() {
      return locks.getTotalLockCount();
   }

   protected abstract void clearLockSafe() throws CacheLoaderException;

   protected abstract Set<StoredEntry> loadAllLockSafe() throws CacheLoaderException;

   protected abstract void toStreamLockSafe(ObjectOutput oos) throws CacheLoaderException;

   protected abstract void fromStreamLockSafe(ObjectInput ois) throws CacheLoaderException;

   protected abstract boolean removeLockSafe(Object key, String lockingKey) throws CacheLoaderException;

   protected abstract void storeLockSafe(StoredEntry ed, String lockingKey) throws CacheLoaderException;

   protected abstract StoredEntry loadLockSafe(Object key, String lockingKey) throws CacheLoaderException;

   protected abstract String getLockFromKey(Object key) throws CacheLoaderException;
}
