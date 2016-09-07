package org.infinispan.lucene.impl;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.store.Lock;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.InvalidLockException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Inter-IndexWriter Lucene index lock based on Infinispan.
 * This implementation is not bound to and does not need a TransactionManager,
 * is more suited for large batch work and index optimization.
 *
 * @since 4.0
 * @author Sanne Grinovero
 * @see org.apache.lucene.store.Lock
 */
@SuppressWarnings("unchecked")
class BaseLuceneLock extends Lock implements Closeable, ObtainableLock {

   private static final Log log = LogFactory.getLog(BaseLuceneLock.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<Object, Object> noCacheStoreCache;
   private final String lockName;
   private final String indexName;
   private final FileCacheKey keyOfLock;
   private final Address valueOfLock;

   BaseLuceneLock(Cache<?, ?> cache, String indexName, String lockName, int affinitySegmentId) {
      this.noCacheStoreCache = (Cache<Object, Object>) cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE, Flag.SKIP_CACHE_LOAD);
      this.lockName = lockName;
      this.indexName = indexName;
      this.keyOfLock = new FileCacheKey(indexName, lockName, affinitySegmentId);
      Address address = noCacheStoreCache.getCacheManager().getAddress();
      this.valueOfLock = address == null ? LocalModeAddress.INSTANCE : address;
   }

   @Override
   public boolean obtain() {
      Object previousValue = noCacheStoreCache.putIfAbsent(keyOfLock, valueOfLock);
      if (trace) log.tracef("Result of lock acquiring %s", previousValue);
      if (previousValue == null) {
         if (trace) {
            log.tracef("Lock: %s acquired for index: %s from %s", lockName, indexName, valueOfLock);
         }
         // we own the lock:
         return true;
      } else {
         if (trace) {
            log.tracef("Lock: %s not acquired for index: %s from %s, was taken already by %s.", lockName,
                    indexName, valueOfLock, previousValue);
         }
         return false;
      }
   }

   public void clearLock() {
      Object lockOwner = noCacheStoreCache.get(keyOfLock);
      if (lockOwner == null || valueOfLock.equals(lockOwner)) {
         Object previousValue = noCacheStoreCache.remove(keyOfLock);
         log.tracef("Lock: %s removed for index: %s from %s (was %s)", lockName, indexName, valueOfLock, previousValue);
      } else {
         log.tracef("Lock: %s not cleared for index: %s from %s, was taken already by %s.", lockName,
               indexName, valueOfLock, lockOwner);
      }
   }

   public boolean isLocked() {
      return noCacheStoreCache.containsKey(keyOfLock);
   }

   /**
    * Since Lucene 4.7, method release() was renamed to close()
    */
   @Override
   public void close() {
      clearLock();
   }

   @Override
   public void ensureValid() throws IOException {
      if (!isLocked()) {
         throw new InvalidLockException("This lock is no longer being held");
      }
   }

}
