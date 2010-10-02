/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.lucene.readlocks;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.FileReadLockKey;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.lucene.InfinispanIndexInput;

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
public class DistributedSegmentReadLocker implements SegmentReadLocker {
   
   private final AdvancedCache cache;
   private final String indexName;
   
   public DistributedSegmentReadLocker(Cache cache, String indexName) {
      if (cache == null)
         throw new IllegalArgumentException("Cache must not be null");
      if (indexName == null)
         throw new IllegalArgumentException("index name must not be null");
      this.indexName = indexName;
      this.cache = cache.getAdvancedCache();
   }

   /**
    * Deletes or releases a read-lock for the specified filename, so that if it was marked as deleted and
    * no other {@link InfinispanIndexInput} instances are reading from it, then it will
    * be effectively deleted.
    * 
    * For removal of readLockKey the SKIP_CACHE_STORE is not used to make sure even
    * values eventually stored by a rehash are cleaned up.
    * 
    * @see #aquireReadLock(String)
    * @see InfinispanDirectory#deleteFile(String)
    */
   @Override
   public void deleteOrReleaseReadLock(String filename) {
      FileReadLockKey readLockKey = new FileReadLockKey(indexName, filename);
      int newValue = 0;
      boolean done = false;
      Object lockValue = cache.withFlags(Flag.SKIP_LOCKING, Flag.SKIP_CACHE_STORE).get(readLockKey);
      while (done == false) {
         if (lockValue == null) {
            lockValue = cache.withFlags(Flag.SKIP_CACHE_STORE).putIfAbsent(readLockKey, Integer.valueOf(0));
            done = (null == lockValue);
         }
         else {
            int refCount = (Integer) lockValue;
            newValue = refCount - 1;
            done = cache.withFlags(Flag.SKIP_CACHE_STORE).replace(readLockKey, refCount, newValue);
            if (!done) {
               lockValue = cache.withFlags(Flag.SKIP_LOCKING, Flag.SKIP_CACHE_STORE).get(readLockKey);
            }
         }
      }
      if (newValue == 0) {
         realFileDelete(readLockKey, cache);
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
    * @param name the name of the "file" for which a readlock is requested 
    * 
    * @see #deleteOrReleaseReadLock(String)
    */
   public boolean aquireReadLock(String filename) {
      FileReadLockKey readLockKey = new FileReadLockKey(indexName, filename);
      Object lockValue = cache.withFlags(Flag.SKIP_LOCKING, Flag.SKIP_CACHE_STORE).get(readLockKey);
      boolean done = false;
      while (done == false) {
         if (lockValue != null) {
            int refCount = (Integer) lockValue;
            if (refCount == 0) {
               // too late: in case refCount==0 the delete is being performed
               return false;
            }
            Integer newValue = Integer.valueOf(refCount + 1);
            done = cache.withFlags(Flag.SKIP_CACHE_STORE).replace(readLockKey, lockValue, newValue);
            if ( ! done) {
               lockValue = cache.withFlags(Flag.SKIP_LOCKING, Flag.SKIP_CACHE_STORE).get(readLockKey);
            }
         } else {
            // readLocks are not stored, so if there's no value assume it's ==1, which means
            // existing file, not deleted, nobody else owning a read lock. but check for ambiguity
            lockValue = cache.withFlags(Flag.SKIP_CACHE_STORE).putIfAbsent(readLockKey, Integer.valueOf(2));
            done = (null == lockValue);
            if (done) {
               // have to check now that the fileKey still exists to prevent the race condition of 
               // T1 fileKey exists - T2 delete file and remove readlock - T1 putIfAbsent(readlock, 2)
               final FileCacheKey fileKey = new FileCacheKey(indexName, filename);
               if (cache.get(fileKey) == null) {
                  cache.withFlags(Flag.SKIP_REMOTE_LOOKUP).removeAsync(readLockKey);
                  return false;
               }
            }
         }
      }
      return true;
   }
   
   /**
    * The {@link InfinispanDirectory#deleteFile(String)} is not deleting the elements from the cache
    * but instead flagging the file as deletable.
    * This method will really remove the elements from the cache; should be invoked only
    * by {@link #releaseReadLock(FileReadLockKey, AdvancedCache)} after having verified that there
    * are no users left in need to read these chunks.
    * 
    * @param readLockKey the key representing the values to be deleted
    * @param cache the cache containing the elements to be deleted
    */
   static void realFileDelete(FileReadLockKey readLockKey, AdvancedCache cache) {
      final String indexName = readLockKey.getIndexName();
      final String filename = readLockKey.getFileName();
      FileCacheKey key = new FileCacheKey(indexName, filename);
      FileMetadata file = (FileMetadata) cache.withFlags(Flag.SKIP_LOCKING).remove(key);
      if (file != null) { //during optimization of index a same file could be deleted twice, so you could see a null here
         for (int i = 0; i < file.getNumberOfChunks(); i++) {
            ChunkCacheKey chunkKey = new ChunkCacheKey(indexName, filename, i);
            cache.withFlags(Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_LOCKING).removeAsync(chunkKey);
         }
      }
      // last operation, as being set as value==0 it prevents others from using it during the
      // deletion process:
      cache.withFlags(Flag.SKIP_REMOTE_LOOKUP).removeAsync(readLockKey);
   }

}
