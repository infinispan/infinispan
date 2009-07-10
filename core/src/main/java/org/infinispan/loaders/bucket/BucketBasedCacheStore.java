package org.infinispan.loaders.bucket;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.LockSupportCacheStore;

/**
 * Base class for cache store that want to use the 'buckets approach' for storing data.
 * <p/>
 * A hashing algorithm is used to map keys to buckets, and a bucket consists of a collection of key/value pairs.
 * <p/>
 * This approach, while adding an overhead of having to search buckets for keys, means that we can use any serializable
 * object we like as keys and not just Strings or objects that translate to something meaningful for a store(e.g. file
 * system).
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class BucketBasedCacheStore extends LockSupportCacheStore {

   /**
    * Loads the bucket coresponding to the given key, and lookups the key within it. if the bucket is found and the key
    * is expired, then it won't be returned.
    *
    * @param key        the passed in key, from {@link LockSupportCacheStore#load(Object)}
    * @param lockingKey the hash of the key, as returned by {@link LockSupportCacheStore#getLockFromKey(Object)}. This
    *                   is present here in order to avoid hash recomputation.
    */
   protected InternalCacheEntry loadLockSafe(Object key, String lockingKey) throws CacheLoaderException {
      Bucket bucket = loadBucket(lockingKey);
      if (bucket == null) return null;
      InternalCacheEntry se = bucket.getEntry(key);

      if (se != null && se.isExpired()) {
         // We do not actually remove expired items from the store here.  We leave that up to the implementation,
         // since it may be a costly thing (remote connection, row locking on a JDBC store for example) for a
         // supposedly quick load operation.
         return null;
      } else {
         return se;
      }
   }

   /**
    * Tries to find a bucket corresponding to storedEntry's key, and updates it with the storedEntry. If no bucket is
    * found, a new one is created.
    *
    * @param lockingKey the hash of the key, as returned by {@link LockSupportCacheStore#getLockFromKey(Object)}. This
    *                   is present here in order to avoid hash recomputation.
    */
   protected void storeLockSafe(InternalCacheEntry ed, String lockingKey) throws CacheLoaderException {
      Bucket bucket = loadBucket(lockingKey);
      if (bucket != null) {
         bucket.addEntry(ed);
         saveBucket(bucket);
      } else {
         bucket = new Bucket();
         bucket.setBucketName(lockingKey);
         bucket.addEntry(ed);
         insertBucket(bucket);
      }
   }

   /**
    * Lookups a bucket where the given key is stored. Then removes the StoredEntry having with gven key from there (if
    * such a bucket exists).
    *
    * @param lockingKey the hash of the key, as returned by {@link LockSupportCacheStore#getLockFromKey(Object)}. This
    *                   is present here in order to avoid hash recomputation.
    */
   protected boolean removeLockSafe(Object key, String lockingKey) throws CacheLoaderException {
      Bucket bucket = loadBucket(lockingKey);
      if (bucket == null) {
         return false;
      } else {
         boolean success = bucket.removeEntry(key);
         if (success) saveBucket(bucket);
         return success;
      }
   }

   /**
    * For {@link BucketBasedCacheStore}s the lock should be acquired at bucket level. So we're locking based on the
    * hashCode of the key, as all keys having same hascode will be mapped to same bucket.
    */
   protected String getLockFromKey(Object key) {
      return String.valueOf(key.hashCode());
   }

   protected abstract void insertBucket(Bucket bucket) throws CacheLoaderException;

   /**
    * This method assumes that the bucket is already persisted in the database.
    *
    * @throws CacheLoaderException if the bucket is not already present, or something happens while persisting.
    */
   protected abstract void saveBucket(Bucket bucket) throws CacheLoaderException;

   /**
    * Loads the bucket from the store, base on the hashcode.
    */
   protected abstract Bucket loadBucket(String keyHashCode) throws CacheLoaderException;
}
