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
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation that uses Infinispan to store Lucene indices.
 * 
 * Directory locking is assured with {@link org.infinispan.lucene.locking.TransactionalSharedLuceneLock}
 * 
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 * @see org.infinispan.lucene.locking.TransactionalLockFactory
 */
// TODO add support for ConcurrentMergeSheduler
public class InfinispanDirectory extends Directory {
   
   // used as default chunk size if not provided in conf
   // each Lucene index segment is splitted into parts with default size defined here
   public final static int DEFAULT_BUFFER_SIZE = 16 * 1024;

   private static final Log log = LogFactory.getLog(InfinispanDirectory.class);

   // own flag required if we are not in this same package what org.apache.lucene.store.Directory,
   // access type will be changed in the next Lucene version
   volatile boolean isOpen = true;

   private final AdvancedCache<CacheKey, Object> cache;
   // indexName is required when one common cache is used
   private final String indexName;
   // chunk size used in this directory, static filed not used as we want to have different chunk
   // size per dir
   private final int chunkSize;

   private final FileListCacheKey fileListCacheKey;

   public InfinispanDirectory(Cache<CacheKey, Object> cache, String indexName, LockFactory lf, int chunkSize) {
      this.cache = cache.getAdvancedCache();
      this.indexName = indexName;
      this.setLockFactory(lf);
      this.chunkSize = chunkSize;
      this.fileListCacheKey = new FileListCacheKey(indexName);
   }

   public InfinispanDirectory(Cache<CacheKey, Object> cache, String indexName, LockFactory lf) {
      this(cache, indexName, lf, DEFAULT_BUFFER_SIZE);
   }

   public InfinispanDirectory(Cache<CacheKey, Object> cache, String indexName, int chunkSize) {
      this(cache, indexName, new BaseLockFactory(cache, indexName), chunkSize);
   }

   public InfinispanDirectory(Cache<CacheKey, Object> cache, String indexName) {
      this(cache, indexName, new BaseLockFactory(cache, indexName), DEFAULT_BUFFER_SIZE);
   }

   public InfinispanDirectory(Cache<CacheKey, Object> cache) {
      this(cache, "");
   }

   /**
    * {@inheritDoc}
    */
   public String[] list() throws IOException {
      checkIsOpen();
      Set<String> filesList = getFileList();
      String[] array = filesList.toArray(new String[0]);
      return array;
   }

   /**
    * {@inheritDoc}
    */
   public boolean fileExists(String name) throws IOException {
      checkIsOpen();
      return cache.withFlags(Flag.SKIP_LOCKING).containsKey(new FileCacheKey(indexName, name));
   }

   /**
    * {@inheritDoc}
    */
   public long fileModified(String name) throws IOException {
      checkIsOpen();
      FileMetadata file = getFileMetadata(name);
      if (file == null) {
         throw new FileNotFoundException(name);
      }
      return file.getLastModified();
   }

   /**
    * {@inheritDoc}
    */
   public void touchFile(String fileName) throws IOException {
      checkIsOpen();
      CacheKey key = new FileCacheKey(indexName, fileName);
      FileMetadata file = (FileMetadata) cache.get(key);
      if (file == null) {
         throw new FileNotFoundException(fileName);
      }
      file.touch();
      cache.put(key, file);
   }

   /**
    * {@inheritDoc}
    */
   public void deleteFile(String name) throws IOException {
      checkIsOpen();
      Set<String> fileList = getFileList();
      boolean deleted = fileList.remove(name);
      if (deleted) {
         cache.put(fileListCacheKey, fileList);
      }
      FileReadLockKey fileReadLockKey = new FileReadLockKey(indexName, name);
      InfinispanIndexInput.releaseReadLock(fileReadLockKey, cache);
      if (log.isDebugEnabled()) {
         log.debug("Removed file: {0} from index: {1}", name, indexName);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void renameFile(String from, String to) throws IOException {
      checkIsOpen();

      // preparation: copy all chunks to new keys
      int i = -1;
      Object ob;
      do {
         ChunkCacheKey fromChunkKey = new ChunkCacheKey(indexName, from, ++i);
         ob = cache.get(fromChunkKey);
         if (ob == null) {
            break;
         }
         ChunkCacheKey toChunkKey = new ChunkCacheKey(indexName, to, i);
         cache.withFlags(Flag.SKIP_REMOTE_LOOKUP).put(toChunkKey, ob);
      } while (true);
      
      // rename metadata first
      cache.startBatch();
      CacheKey fromKey = new FileCacheKey(indexName, from);
      FileMetadata metadata = (FileMetadata) cache.remove(fromKey);
      cache.put(new FileCacheKey(indexName, to), metadata);
      Set<String> fileList = getFileList();
      fileList.remove(from);
      fileList.add(to);
      createRefCountForNewFile(to);
      cache.put(fileListCacheKey, fileList);
      cache.endBatch(true);
      
      // now trigger deletion of old file chunks:
      FileReadLockKey fileFromReadLockKey = new FileReadLockKey(indexName, from);
      InfinispanIndexInput.releaseReadLock(fileFromReadLockKey, cache);
      if (log.isTraceEnabled()) {
         log.trace("Renamed file from: {0} to: {1} in index {2}", from, to, indexName);
      }
   }

   /**
    * {@inheritDoc}
    */
   public long fileLength(String name) throws IOException {
      checkIsOpen();
      final FileMetadata file = getFileMetadata(name);
      if (file == null) {
         throw new FileNotFoundException(name);
      }
      return file.getSize();
   }

   /**
    * {@inheritDoc}
    */
   public IndexOutput createOutput(String name) throws IOException {
      final FileCacheKey key = new FileCacheKey(indexName, name);
      FileMetadata newFileMetadata = new FileMetadata();
      FileMetadata previous = (FileMetadata) cache.putIfAbsent(key, newFileMetadata);
      if (previous == null) {
         // creating new file
         createRefCountForNewFile(name);
         Set<String> fileList = getFileList();
         fileList.add(name);
         cache.put(fileListCacheKey, fileList);
         return new InfinispanIndexOutput(cache, key, chunkSize, newFileMetadata);
      } else {
         return new InfinispanIndexOutput(cache, key, chunkSize, previous);
      }
   }

   private void createRefCountForNewFile(String fileName) {
      FileReadLockKey readLockKey = new FileReadLockKey(indexName, fileName);
      cache.withFlags(Flag.SKIP_REMOTE_LOOKUP).put(readLockKey, Integer.valueOf(1));
   }

   @SuppressWarnings("unchecked")
   private Set<String> getFileList() {
      Set<String> fileList = (Set<String>) cache.withFlags(Flag.SKIP_LOCKING).get(fileListCacheKey);
      if (fileList == null)
         fileList = new ConcurrentHashSet<String>();
      return fileList;
   }

   /**
    * {@inheritDoc}
    */
   public IndexInput openInput(String name) throws IOException {
      final FileCacheKey fileKey = new FileCacheKey(indexName, name);
      return new InfinispanIndexInput(cache, fileKey, chunkSize);
   }

   /**
    * {@inheritDoc}
    */
   public void close() throws IOException {
      isOpen = false;
   }

   private void checkIsOpen() throws AlreadyClosedException {
      if (!isOpen) {
         throw new AlreadyClosedException("this Directory is closed");
      }
   }

   private FileMetadata getFileMetadata(String fileName) {
      CacheKey key = new FileCacheKey(indexName, fileName);
      return (FileMetadata) cache.withFlags(Flag.SKIP_LOCKING).get(key);
   }

   @Override
   public String toString() {
      return "InfinispanDirectory{" + "indexName='" + indexName + '\'' + '}';
   }

   public Cache<CacheKey, Object> getCache() {
      return cache;
   }

   /** new name for list() in Lucene 3.0 **/
   public String[] listAll() throws IOException {
      return list();
   }
   
}
