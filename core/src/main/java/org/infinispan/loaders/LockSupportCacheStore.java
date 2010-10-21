package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.concurrent.locks.StripedLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

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
 */
public abstract class LockSupportCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(LockSupportCacheStore.class);
   private static final boolean trace = log.isTraceEnabled();

   private StripedLock locks;
   private long globalLockTimeoutMillis;
   private LockSupportCacheStoreConfig config;

   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (LockSupportCacheStoreConfig) config;
   }

   public void start() throws CacheLoaderException {
      super.start();
      if (config == null)
         throw new CacheLoaderException("Null config. Possible reason is not calling super.init(...)");
      if (log.isTraceEnabled()) {
         log.trace("Starting cache with config:" + config);
      }

      locks = new StripedLock(config.getLockConcurrencyLevel());
      globalLockTimeoutMillis = config.getLockAcquistionTimeout();
   }

   /**
    * Release the locks (either read or write).
    */
   protected final void unlock(String key) {
      locks.releaseLock(key);
   }

   /**
    * Acquires write lock on the given key.
    */
   protected final void lockForWriting(String key) throws CacheLoaderException {
      locks.acquireLock(key, true);
   }

   /**
    * Acquires read lock on the given key.
    */
   protected final void lockForReading(String key) throws CacheLoaderException {
      locks.acquireLock(key, false);
   }

   /**
    * Same as {@link #lockForWriting(String)}, but with 0 timeout.
    */
   protected final boolean immediateLockForWriting(String key) throws CacheLoaderException {
      return locks.acquireLock(key, true, 0);
   }

   /**
    * Based on the supplied param, acquires a global read(false) or write (true) lock.
    */
   protected final boolean acquireGlobalLock(boolean exclusive) throws CacheLoaderException {
      return locks.aquireGlobalLock(exclusive, globalLockTimeoutMillis);
   }

   /**
    * Based on the supplied param, releases a global read(false) or write (true) lock.
    */
   protected final void releaseGlobalLock(boolean exclusive) {
      locks.releaseGlobalLock(exclusive);
   }

   public final InternalCacheEntry load(Object key) throws CacheLoaderException {
      String lockingKey = getLockFromKey(key);
      lockForReading(lockingKey);
      try {
         return loadLockSafe(key, lockingKey);
      } finally {
         unlock(lockingKey);
      }
   }

   public final Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      acquireGlobalLock(false);
      try {
         return loadAllLockSafe();
      } finally {
         releaseGlobalLock(false);
      }
   }

   public final Set<InternalCacheEntry> load(int maxEntries) throws CacheLoaderException {
      if (maxEntries < 0) return loadAll();
      acquireGlobalLock(false);
      try {
         return loadLockSafe(maxEntries);
      } finally {
         releaseGlobalLock(false);
      }
   }

   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      acquireGlobalLock(false);
      try {
         return loadAllKeysLockSafe(keysToExclude);
      } finally {
         releaseGlobalLock(false);
      }
   }


   public final void store(InternalCacheEntry ed) throws CacheLoaderException {
      if (trace) log.trace("store(" + ed + ")");
      if (ed == null) return;
      if (ed.isExpired()) {
         if (containsKey(ed.getKey())) {
            if (trace) log.trace("Entry " + ed + " is expired!  Removing!");
            remove(ed.getKey());
         } else {
            if (trace) log.trace("Entry " + ed + " is expired!  Not doing anything.");
         }
         return;
      }

      String keyHashCode = getLockFromKey(ed.getKey());
      lockForWriting(keyHashCode);
      try {
         storeLockSafe(ed, keyHashCode);
      } finally {
         unlock(keyHashCode);
      }
      if (trace) log.trace("exit store(" + ed + ")");
   }

   public final boolean remove(Object key) throws CacheLoaderException {
      if (trace) log.trace("remove(" + key + ")");
      String keyHashCodeStr = getLockFromKey(key);
      try {
         lockForWriting(keyHashCodeStr);
         return removeLockSafe(key, keyHashCodeStr);
      } finally {
         unlock(keyHashCodeStr);
         if (trace) log.trace("Exit remove(" + key + ")");
      }
   }

   public final void fromStream(ObjectInput objectInput) throws CacheLoaderException {
      try {
         acquireGlobalLock(true);
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
      if (trace) log.trace("Clearing store");
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

   protected abstract Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException;

   protected abstract Set<InternalCacheEntry> loadLockSafe(int maxEntries) throws CacheLoaderException;

   protected abstract Set<Object> loadAllKeysLockSafe(Set<Object> keysToExclude) throws CacheLoaderException;

   protected abstract void toStreamLockSafe(ObjectOutput oos) throws CacheLoaderException;

   protected abstract void fromStreamLockSafe(ObjectInput ois) throws CacheLoaderException;

   protected abstract boolean removeLockSafe(Object key, String lockingKey) throws CacheLoaderException;

   protected abstract void storeLockSafe(InternalCacheEntry ed, String lockingKey) throws CacheLoaderException;

   protected abstract InternalCacheEntry loadLockSafe(Object key, String lockingKey) throws CacheLoaderException;

   protected abstract String getLockFromKey(Object key) throws CacheLoaderException;
}
