package org.infinispan.lucene.readlocks;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.FileReadLockKey;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * <p>DistributedSegmentReadLocker stores reference counters in the cache
 * to keep track of the number of clients still needing to be able
 * to read a segment. It makes extensive usage of Infinispan's atomic
 * operations.</p>
 * <p>Locks stored this way are not optimally performing as it might spin
 * on remote invocations, and might fail to cleanup some garbage
 * in case a node is disconnected without having released the readlock.</p>
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
@SuppressWarnings("unchecked")
public class DistributedSegmentReadLocker implements SegmentReadLocker {

   private static final Log log = LogFactory.getLog(DistributedSegmentReadLocker.class);

   private final AdvancedCache<Object, Integer> locksCache;
   private final AdvancedCache<?, ?> chunksCache;
   private final AdvancedCache<FileCacheKey, FileMetadata> metadataCache;
   private final String indexName;
   private final boolean forceSynchronousDeletes;

   public DistributedSegmentReadLocker(Cache<Object, Integer> locksCache, Cache<?, ?> chunksCache, Cache<?, ?> metadataCache, String indexName) {
      this(locksCache, chunksCache, metadataCache, indexName, false);
   }

   public DistributedSegmentReadLocker(Cache<Object, Integer> locksCache, Cache<?, ?> chunksCache, Cache<?, ?> metadataCache, String indexName, boolean forceSynchronousDeletes) {
      this.forceSynchronousDeletes = forceSynchronousDeletes;
      if (locksCache == null)
         throw new IllegalArgumentException("locksCache must not be null");
      if (chunksCache == null)
         throw new IllegalArgumentException("chunksCache must not be null");
      if (metadataCache == null)
         throw new IllegalArgumentException("metadataCache must not be null");
      if (indexName == null)
         throw new IllegalArgumentException("index name must not be null");
      this.indexName = indexName;
      this.locksCache = locksCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE, Flag.SKIP_CACHE_LOAD, Flag.SKIP_INDEXING);
      this.chunksCache = chunksCache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
      this.metadataCache = (AdvancedCache<FileCacheKey, FileMetadata>) metadataCache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
      verifyCacheHasNoEviction(this.locksCache);
   }

   public DistributedSegmentReadLocker(Cache<?, ?> cache, String indexName) {
      this((Cache<Object, Integer>) cache, cache, cache, indexName);
   }

   /**
    * Deletes or releases a read-lock for the specified filename, so that if it was marked as deleted and
    * no other {@link org.infinispan.lucene.impl.InfinispanIndexInput} instances are reading from it, then it will
    * be effectively deleted.
    *
    * @see #acquireReadLock(String)
    * @see org.apache.lucene.store.Directory#deleteFile(String)
    */
   @Override
   public void deleteOrReleaseReadLock(final String filename) {
      if (isMultiChunked(filename)) {
         int newValue = 0;
         FileReadLockKey readLockKey = new FileReadLockKey(indexName, filename);
         boolean done = false;
         Object lockValue = locksCache.get(readLockKey);
         while (!done) {
            if (lockValue == null) {
               lockValue = locksCache.putIfAbsent(readLockKey, 0);
               done = (null == lockValue);
            }
            else {
               Integer refCountObject = (Integer) lockValue;
               int refCount = refCountObject.intValue();
               newValue = refCount - 1;
               done = locksCache.replace(readLockKey, refCountObject, newValue);
               if (!done) {
                  lockValue = locksCache.get(readLockKey);
               }
            }
         }
         if (newValue == 0) {
            realFileDelete(indexName, filename, locksCache, chunksCache, metadataCache, forceSynchronousDeletes);
         }
      }
      else {
         realFileDelete(indexName, filename, locksCache, chunksCache, metadataCache, forceSynchronousDeletes);
      }
   }

   /**
    * Evaluates if the file is potentially being stored as fragmented into multiple chunks;
    * when it's a single chunk we don't need to apply readlocks.
    * @param filename
    * @return true if it is definitely fragmented, or if it's possibly fragmented.
    */
   private boolean isMultiChunked(final String filename) {
      final FileCacheKey fileCacheKey = new FileCacheKey(indexName, filename);
      final FileMetadata fileMetadata = metadataCache.get(fileCacheKey);
      if (fileMetadata==null) {
         //This might happen under high load when the metadata is being written
         //using putAsync; in such case we return true as it's the safest option.
         //Skipping the readlocks is just a performance optimisation, and this
         //condition is extremely rare.
         return true;
      }
      else {
         return fileMetadata.isMultiChunked();
      }
   }

   /**
    * Acquires a readlock on all chunks for this file, to make sure chunks are not deleted while
    * iterating on the group. This is needed to avoid an eager lock on all elements.
    *
    * If no value is found in the cache, a disambiguation procedure is needed: not value
    * might mean both "existing, no readlocks, no deletions in progress", but also "not existent file".
    * The first possibility is coded as no value to avoid storing readlocks in a permanent store,
    * which would unnecessarily slow down and provide unwanted long term storage of the lock;
    * so the value is treated as one if not found, but obviously it's also not found for non-existent
    * or concurrently deleted files.
    *
    * @param filename the name of the "file" for which a readlock is requested
    *
    * @see #deleteOrReleaseReadLock(String)
    */
   @Override
   public boolean acquireReadLock(String filename) {
      FileReadLockKey readLockKey = new FileReadLockKey(indexName, filename);
      Integer lockValue = locksCache.get(readLockKey);
      boolean done = false;
      while (done == false) {
         if (lockValue != null) {
            int refCount = lockValue.intValue();
            if (refCount == 0) {
               // too late: in case refCount==0 the delete is being performed
               return false;
            }
            Integer newValue = Integer.valueOf(refCount + 1);
            done = locksCache.replace(readLockKey, lockValue, newValue);
            if ( ! done) {
               lockValue = locksCache.get(readLockKey);
            }
         } else {
            // readLocks are not stored, so if there's no value assume it's ==1, which means
            // existing file, not deleted, nobody else owning a read lock. but check for ambiguity
            lockValue = locksCache.putIfAbsent(readLockKey, 2);
            done = (null == lockValue);
            if (done) {
               // have to check now that the fileKey still exists to prevent the race condition of
               // T1 fileKey exists - T2 delete file and remove readlock - T1 putIfAbsent(readlock, 2)
               final FileCacheKey fileKey = new FileCacheKey(indexName, filename);
               if (metadataCache.get(fileKey) == null) {
                  locksCache.withFlags(Flag.IGNORE_RETURN_VALUES).removeAsync(readLockKey);
                  return false;
               }
            }
         }
      }
      return true;
   }

   /**
    * The {@link org.apache.lucene.store.Directory#deleteFile(String)} is not deleting the elements from the cache
    * but instead flagging the file as deletable.
    * This method will really remove the elements from the cache; should be invoked only
    * by {@link #deleteOrReleaseReadLock(String)} after having verified that there
    * are no users left in need to read these chunks.
    *
    * @param indexName the index name to delete the file from
    * @param fileName the name of the file to delete
    * @param locksCache the cache containing the locks
    * @param chunksCache the cache containing the chunks to be deleted
    * @param metadataCache the cache containing the metadata of elements to be deleted
    * @param forceSynchronousDeletes when false deletion of chunk data is performed asynchronously
    */
   static void realFileDelete(String indexName, String fileName, AdvancedCache<Object, Integer> locksCache,
                              AdvancedCache<?, ?> chunksCache, AdvancedCache<?, ?> metadataCache, boolean forceSynchronousDeletes) {
      final boolean trace = log.isTraceEnabled();
      final FileCacheKey key = new FileCacheKey(indexName, fileName);
      if (trace) log.tracef("deleting metadata: %s", key);
      final FileMetadata file = (FileMetadata) metadataCache.remove(key);
      if (file != null) { //during optimization of index a same file could be deleted twice, so you could see a null here
         final int bufferSize = file.getBufferSize();
         AdvancedCache<?, ?> chunksCacheNoReturn = chunksCache.withFlags(Flag.IGNORE_RETURN_VALUES);
         for (int i = 0; i < file.getNumberOfChunks(); i++) {
            ChunkCacheKey chunkKey = new ChunkCacheKey(indexName, fileName, i, bufferSize);
            if (trace) log.tracef("deleting chunk: %s", chunkKey);
            if (forceSynchronousDeletes) {
               chunksCacheNoReturn.remove(chunkKey);
            }
            else {
               chunksCacheNoReturn.removeAsync(chunkKey);
            }
         }
      }
      // last operation, as being set as value==0 it prevents others from using it during the
      // deletion process:
      if (file != null && file.isMultiChunked()) {
         FileReadLockKey readLockKey = new FileReadLockKey(indexName, fileName);
         if (trace) log.tracef("deleting readlock: %s", readLockKey);
         if (forceSynchronousDeletes) {
            locksCache.withFlags(Flag.IGNORE_RETURN_VALUES).remove(readLockKey);
         } else {
            locksCache.withFlags(Flag.IGNORE_RETURN_VALUES).removeAsync(readLockKey);
         }
      }
   }

   private static void verifyCacheHasNoEviction(AdvancedCache<?, ?> cache) {
      if (cache.getCacheConfiguration().eviction().strategy().isEnabled())
         throw new IllegalArgumentException("DistributedSegmentReadLocker is not reliable when using a cache with eviction enabled, disable eviction on this cache instance");
   }

}
