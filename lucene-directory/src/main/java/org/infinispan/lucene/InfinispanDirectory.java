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
package org.infinispan.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.locking.BaseLockFactory;
import org.infinispan.lucene.readlocks.DistributedSegmentReadLocker;
import org.infinispan.lucene.readlocks.SegmentReadLocker;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An implementation of Lucene's {@link org.apache.lucene.store.Directory} which uses Infinispan to store Lucene indexes.
 * As the RAMDirectory the data is stored in memory, but provides some additional flexibility:
 * <p><b>Passivation, LRU or LIRS</b> Bigger indexes can be configured to passivate cleverly selected chunks of data to a cache store.
 * This can be a local filesystem, a network filesystem, a database or custom cloud stores like S3. See Infinispan's core documentation for a full list of available implementations, or {@link org.infinispan.loaders.CacheStore} to implement more.</p>
 * <p><b>Non-volatile memory</b> The contents of the index can be stored in it's entirety in such a store, so that on shutdown or crash of the system data is not lost.
 * A copy of the index will be copied to the store in sync or async depending on configuration; In case you enable
 * Infinispan's clustering even in case of async the segments are always duplicated synchronously to other nodes, so you can
 * benefit from good reliability even while choosing the asynchronous mode to write the index to the slowest store implementations.</p>
 * <p><b>Real-time change propagation</b> All changes done on a node are propagated at low latency to other nodes of the cluster; this was designed especially for
 * interactive usage of Lucene, so that after an IndexWriter commits on one node new IndexReaders opened on any node of the cluster
 * will be able to deliver updated search results.</p>
 * <p><b>Distributed heap</b> Infinispan acts as a shared heap for the purpose of total memory consumption, so you can avoid hitting the slower disks even
 * if the total size of the index can't fit in the memory of a single node: network is faster than disks, especially if the index
 * is bigger than the memory available to cache it.</p>
 * <p><b>Distributed locking</b>
 * As default Lucene Directory implementations a global lock needs to protect the index from having more than an IndexWriter open; in case of a
 * replicated or distributed index you need to enable a cluster-wide {@link org.apache.lucene.store.LockFactory}.
 * This implementation uses by default {@link org.infinispan.lucene.locking.BaseLockFactory}; in case you want to apply changes during a JTA transaction
 * see also {@link org.infinispan.lucene.locking.TransactionalLockFactory}.
 * </p>
 * <p><b>Combined store patterns</b> It's possible to combine different stores and passivation policies, so that each nodes shares the index changes
 * quickly to other nodes, offloads less frequently used data to a per-node local filesystem, and the cluster also coordinates to keeps a safe copy on a shared store.</p>
 * 
 * @since 4.0
 * @author Sanne Grinovero
 * @author Lukasz Moren
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.LockFactory
 * @see org.infinispan.lucene.locking.BaseLockFactory
 * @see org.infinispan.lucene.locking.TransactionalLockFactory
 */
@SuppressWarnings("unchecked")
public class InfinispanDirectory extends Directory {
   
   /**
    * Used as default chunk size, can be overriden at construction time.
    * Each Lucene index segment is splitted into parts with default size defined here
    */
   public final static int DEFAULT_BUFFER_SIZE = 16 * 1024;

   private static final Log log = LogFactory.getLog(InfinispanDirectory.class);

   // own flag required if we are not in this same package what org.apache.lucene.store.Directory,
   // access type will be changed in the next Lucene version
   volatile boolean isOpen = true;

   private final AdvancedCache metadataCache;
   private final AdvancedCache chunksCache;
   // indexName is required when one common cache is used
   private final String indexName;
   // chunk size used in this directory, static filed not used as we want to have different chunk
   // size per dir
   private final int chunkSize;

   private final FileListOperations fileOps;
   private final SegmentReadLocker readLocks;

   /**
    * @param metadataCache the cache to be used for all smaller metadata: prefer replication over distribution, avoid eviction
    * @param chunksCache the cache to use for the space consuming segments: prefer distribution, enable eviction if needed
    * @param indexName the unique index name, useful to store multiple indexes in the same caches
    * @param lf the LockFactory to be used by IndexWriters. @see org.infinispan.lucene.locking
    * @param chunkSize segments are fragmented in chunkSize bytes; larger values are more efficient for searching but less for distribution and network replication
    * @param readLocker @see org.infinispan.lucene.readlocks for some implementations; you might be able to provide more efficient implementations by controlling the IndexReader's lifecycle.
    */
   public InfinispanDirectory(Cache metadataCache, Cache chunksCache, String indexName, LockFactory lf, int chunkSize, SegmentReadLocker readLocker) {
      checkNotNull(metadataCache, "metadataCache");
      checkNotNull(chunksCache, "chunksCache");
      checkNotNull(indexName, "indexName");
      checkNotNull(lf, "LockFactory");
      checkNotNull(readLocker, "SegmentReadLocker");
      if (chunkSize <= 0)
         throw new IllegalArgumentException("chunkSize must be a positive integer");
      this.metadataCache = metadataCache.getAdvancedCache();
      this.chunksCache = chunksCache.getAdvancedCache();
      this.indexName = indexName;
      this.lockFactory = lf;
      this.lockFactory.setLockPrefix(this.getLockID());
      this.chunkSize = chunkSize;
      this.fileOps = new FileListOperations(this.metadataCache, indexName);
      this.readLocks = readLocker;
   }

   public InfinispanDirectory(Cache cache, String indexName, int chunkSize, SegmentReadLocker readLocker) {
      this(cache, cache, indexName, makeDefaultLockFactory(cache, indexName), chunkSize, readLocker);
   }

   /**
    * This constructor assumes that three different caches are being used with specialized configurations for each
    * cache usage
    * @param metadataCache contains the metadata of stored elements
    * @param chunksCache cache containing the bulk of the index; this is the larger part of data
    * @param distLocksCache cache to store locks; should be replicated and not using a persistent CacheStore
    * @param indexName identifies the index; you can store different indexes in the same set of caches using different identifiers
    * @param chunkSize the maximum size in bytes for each chunk of data: larger sizes offer better search performance
    * but might be problematic to handle during network replication or storage
    */
   public InfinispanDirectory(Cache metadataCache, Cache chunksCache, Cache distLocksCache, String indexName, int chunkSize) {
      this(metadataCache, chunksCache, indexName, makeDefaultLockFactory(distLocksCache, indexName),
               chunkSize, makeDefaultSegmentReadLocker(metadataCache, chunksCache, distLocksCache, indexName));
   }

   /**
    * @param cache the cache to use to store the index
    * @param indexName identifies the index; you can store different indexes in the same set of caches using different identifiers
    */
   public InfinispanDirectory(Cache cache, String indexName) {
      this(cache, cache, cache, indexName, DEFAULT_BUFFER_SIZE);
   }

   public InfinispanDirectory(Cache cache) {
      this(cache, cache, cache, "", DEFAULT_BUFFER_SIZE);
   }

   /**
    * {@inheritDoc}
    */
   public String[] list() {
      checkIsOpen();
      Set<String> filesList = fileOps.getFileList();
      String[] array = filesList.toArray(new String[0]);
      return array;
   }

   /**
    * {@inheritDoc}
    */
   public boolean fileExists(String name) {
      checkIsOpen();
      return fileOps.getFileList().contains(name);
   }

   /**
    * {@inheritDoc}
    */
   public long fileModified(String name) {
      checkIsOpen();
      FileMetadata fileMetadata = fileOps.getFileMetadata(name);
      if (fileMetadata == null) {
         return 0L;
      }
      else {
         return fileMetadata.getLastModified();
      }
   }

   /**
    * {@inheritDoc}
    */
   public void touchFile(String fileName) {
      checkIsOpen();
      FileMetadata file = fileOps.getFileMetadata(fileName);
      if (file == null) {
         return;
      }
      else {
         FileCacheKey key = new FileCacheKey(indexName, fileName);
         file.touch();
         metadataCache.put(key, file);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void deleteFile(String name) {
      checkIsOpen();
      fileOps.deleteFileName(name);
      readLocks.deleteOrReleaseReadLock(name);
      if (log.isDebugEnabled()) {
         log.debugf("Removed file: %s from index: %s", name, indexName);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void renameFile(String from, String to) {
      checkIsOpen();

      // preparation: copy all chunks to new keys
      int i = -1;
      Object ob;
      do {
         ChunkCacheKey fromChunkKey = new ChunkCacheKey(indexName, from, ++i);
         ob = chunksCache.get(fromChunkKey);
         if (ob == null) {
            break;
         }
         ChunkCacheKey toChunkKey = new ChunkCacheKey(indexName, to, i);
         chunksCache.withFlags(Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_CACHE_LOAD).put(toChunkKey, ob);
      } while (true);
      
      // rename metadata first
      boolean batching = metadataCache.startBatch();
      FileCacheKey fromKey = new FileCacheKey(indexName, from);
      FileMetadata metadata = (FileMetadata) metadataCache.withFlags(Flag.SKIP_LOCKING).get(fromKey);
      metadataCache.put(new FileCacheKey(indexName, to), metadata);
      fileOps.removeAndAdd(from, to);
      if (batching) metadataCache.endBatch(true);
      
      // now trigger deletion of old file chunks:
      readLocks.deleteOrReleaseReadLock(from);
      if (log.isTraceEnabled()) {
         log.tracef("Renamed file from: %s to: %s in index %s", from, to, indexName);
      }
   }

   /**
    * {@inheritDoc}
    */
   public long fileLength(String name) {
      checkIsOpen();
      FileMetadata fileMetadata = fileOps.getFileMetadata(name);
      if (fileMetadata == null) {
         return 0L;//as in FSDirectory (RAMDirectory throws an exception instead)
      }
      else {
         return fileMetadata.getSize();
      }
   }

   /**
    * {@inheritDoc}
    */
   public IndexOutput createOutput(String name) {
      final FileCacheKey key = new FileCacheKey(indexName, name);
      // creating new file, metadata is added on flush() or close() of IndexOutPut
      return new InfinispanIndexOutput(metadataCache, chunksCache, key, chunkSize, fileOps);
   }

   /**
    * {@inheritDoc}
    */
   public IndexInput openInput(String name) throws IOException {
      final FileCacheKey fileKey = new FileCacheKey(indexName, name);
      FileMetadata fileMetadata = (FileMetadata) metadataCache.withFlags(Flag.SKIP_LOCKING).get(fileKey);
      if (fileMetadata == null) {
         throw new FileNotFoundException("Error loading medatada for index file: " + fileKey);
      }
      else if (fileMetadata.getSize() <= fileMetadata.getBufferSize()) {
         //files smaller than chunkSize don't need a readLock
         return new SingleChunkIndexInput(chunksCache, fileKey, fileMetadata);
      }
      else {
         boolean locked = readLocks.acquireReadLock(name);
         if (!locked) {
            // safest reaction is to tell this file doesn't exist anymore.
            throw new FileNotFoundException("Error loading medatada for index file: " + fileKey);
         }
         return new InfinispanIndexInput(chunksCache, fileKey, fileMetadata, readLocks);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void close() {
      isOpen = false;
   }

   private void checkIsOpen() throws AlreadyClosedException {
      if (!isOpen) {
         throw new AlreadyClosedException("this Directory is closed");
      }
   }

   @Override
   public String toString() {
      return "InfinispanDirectory{" + "indexName='" + indexName + '\'' + '}';
   }

   /** new name for list() in Lucene 3.0 **/
   public String[] listAll() {
      return list();
   }

   /**
    * @return The value of indexName, same constant as provided to the constructor.
    */
   public String getIndexName() {
       return indexName;
   }
   
   private static LockFactory makeDefaultLockFactory(Cache cache, String indexName) {
      checkNotNull(cache, "cache");
      checkNotNull(indexName, "indexName");
      return new BaseLockFactory(cache, indexName);
   }
   
   private static SegmentReadLocker makeDefaultSegmentReadLocker(Cache metadataCache, Cache chunksCache, Cache distLocksCache, String indexName) {
      checkNotNull(distLocksCache, "distLocksCache");
      checkNotNull(indexName, "indexName");
      return new DistributedSegmentReadLocker(distLocksCache, chunksCache, metadataCache, indexName);
   }
   
   private static void checkNotNull(Object v, String objectname) {
      if (v == null)
         throw new IllegalArgumentException(objectname + " must not be null");
   }
   
}
