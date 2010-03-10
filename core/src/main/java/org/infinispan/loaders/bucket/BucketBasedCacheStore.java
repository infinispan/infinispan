package org.infinispan.loaders.bucket;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.LockSupportCacheStore;

/**
 * Base class for CacheStore implementations that combine entries into buckets when storing data.
 * <p/>
 * A hashing algorithm is used to map keys to buckets, and a bucket consists of a collection of key/value pairs.
 * <p/>
 * This approach, while adding an overhead of having to search through the contents of buckets a relevant entry,
 * allows us to use any Serializable object as a key since the bucket is identified by a hash code.  This hash code
 * is often easy to represent in a physical store, such as a file system, database, etc.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class BucketBasedCacheStore extends LockSupportCacheStore {

   /**
    * Loads an entry from a Bucket, locating the relevant Bucket using the key's hash code.
    *
    * @param key        key of the entry to remove.
    * @param lockingKey the hash of the key, as returned by {@link LockSupportCacheStore#getLockFromKey(Object)}. This
    *                   is required in order to avoid hash re-computation.
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
    * Stores an entry in an appropriate Bucket, based on the key's hash code.  If the Bucket does not exist in the
    * underlying store, a new one is created.
    *
    * @param entry      the entry to store
    * @param lockingKey the hash of the key, as returned by {@link LockSupportCacheStore#getLockFromKey(Object)}. This
    *                   is required in order to avoid hash re-computation.
    */
   protected void storeLockSafe(InternalCacheEntry entry, String lockingKey) throws CacheLoaderException {
      Bucket bucket = loadBucket(lockingKey);
      if (bucket != null) {
         bucket.addEntry(entry);
         updateBucket(bucket);
      } else {
         bucket = new Bucket();
         bucket.setBucketName(lockingKey);
         bucket.addEntry(entry);
         insertBucket(bucket);
      }
   }

   /**
    * Removes an entry from a Bucket, locating the relevant Bucket using the key's hash code.
    * @param key        key of the entry to remove.
    * @param lockingKey the hash of the key, as returned by {@link LockSupportCacheStore#getLockFromKey(Object)}. This
    *                   is required in order to avoid hash re-computation.
    */
   protected boolean removeLockSafe(Object key, String lockingKey) throws CacheLoaderException {
      Bucket bucket = loadBucket(lockingKey);
      if (bucket == null) {
         return false;
      } else {
         boolean success = bucket.removeEntry(key);
         if (success) updateBucket(bucket);
         return success;
      }
   }

   /**
    * For {@link BucketBasedCacheStore}s the lock should be acquired at bucket level. So we're locking based on the
    * hash code of the key, as all keys having same hash code will be mapped to same bucket.
    */
   protected String getLockFromKey(Object key) {
      return String.valueOf(key.hashCode());
   }

   /**
    * Inserts a new Bucket in the storage system.  If the bucket already exists, this method should simply update the
    * store with the contents of the bucket - i.e., behave the same as {@link #updateBucket(Bucket)}.
    *
    * @param bucket bucket to insert
    * @throws CacheLoaderException in case of problems with the store.
    */
   protected void insertBucket(Bucket bucket) throws CacheLoaderException {
      // the default behavior is to assume that updateBucket() will create a new bucket, so we just forward calls to
      // updateBucket().
      updateBucket(bucket);
   }

   /**
    * Updates a bucket in the store with the Bucket passed in to the method.  This method assumes that the bucket
    * already exists in the store, however some implementations may choose to simply create a new bucket if the bucket
    * does not exist.
    * <p />
    * The default behavior is that non-existent buckets are created on the fly.  If this is <i>not</i> the case in your
    * implementation, then you would have to override {@link #insertBucket(Bucket)} as well so that it doesn't blindly
    * forward calls to {@link #updateBucket(Bucket)}.
    * <p />
    * @param bucket bucket to update.
    * @throws CacheLoaderException in case of problems with the store.
    */
   protected abstract void updateBucket(Bucket bucket) throws CacheLoaderException;

   /**
    * Loads a Bucket from the store, based on the hash code of the bucket.
    * @param hash String representation of the Bucket's hash
    * @return a Bucket if one exists, null otherwise.
    * @throws CacheLoaderException in case of problems with the store.
    */
   protected abstract Bucket loadBucket(String hash) throws CacheLoaderException;
}
