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

import org.apache.lucene.store.IndexInput;
import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Responsible for reading from <code>InfinispanDirectory</code>
 * 
 * @since 4.0
 * @author Sanne Grinovero
 * @author Davide Di Somma
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.IndexInput
 */
public class InfinispanIndexInput extends IndexInput {

   private static final Log log = LogFactory.getLog(InfinispanIndexInput.class);

   private final AdvancedCache<CacheKey, Object> cache;
   private final FileMetadata file;
   private final FileCacheKey fileKey;
   private final int chunkSize;
   private final FileReadLockKey readLockKey;

   private int currentBufferSize;
   private byte[] buffer;
   private int bufferPosition;
   private int currentLoadedChunk = -1;

   private boolean isClone;

   public InfinispanIndexInput(AdvancedCache<CacheKey, Object> cache, FileCacheKey fileKey, int chunkSize) throws FileNotFoundException {
      this.cache = cache;
      this.fileKey = fileKey;
      this.chunkSize = chunkSize;
      final String filename = fileKey.getFileName();
      this.readLockKey = new FileReadLockKey(fileKey.getIndexName(), filename);
      
      boolean failure = true;
      aquireReadLock();
      try {

         // get file header from file
         this.file = (FileMetadata) cache.withFlags(Flag.SKIP_LOCKING).get(fileKey);

         if (file == null) {
            throw new FileNotFoundException("Error loading medatada for index file: " + fileKey);
         }

         if (log.isDebugEnabled()) {
            log.debug("Opened new IndexInput for file:{0} in index: {1}", filename, fileKey.getIndexName());
         }
         failure = false;
      } finally {
         if (failure)
            releaseReadLock();
      }
   }

   private void releaseReadLock() {
      releaseReadLock(readLockKey, cache);
   }
   
   /**
    * Releases a read-lock for this file, so that if it was marked as deleted and
    * no other {@link InfinispanIndexInput} instances are using it, then it will
    * be effectively deleted.
    * 
    * @see #aquireReadLock()
    * @see InfinispanDirectory#deleteFile(String)
    * 
    * @param readLockKey the key pointing to the reference counter value
    * @param cache The cache containing the reference counter value
    */
   static void releaseReadLock(FileReadLockKey readLockKey, AdvancedCache<CacheKey, Object> cache) {
      int newValue = 0;
      // spinning as we currently don't mandate transactions, so no proper lock support available
      boolean done = false;
      while (done == false) {
         Object lockValue = cache.get(readLockKey);
         if (lockValue == null)
            return; // no special locking for some core files
         int refCount = (Integer) lockValue;
         newValue = refCount - 1;
         done = cache.replace(readLockKey, refCount, newValue);
      }
      if (newValue == 0) {
         realFileDelete(readLockKey, cache);
      }
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
   static void realFileDelete(FileReadLockKey readLockKey, AdvancedCache<CacheKey, Object> cache) {
      final String indexName = readLockKey.getIndexName();
      final String filename = readLockKey.getFileName();
      int i = 0;
      Object removed;
      ChunkCacheKey chunkKey = new ChunkCacheKey(indexName, filename, i);
      do {
         removed = cache.withFlags(Flag.SKIP_LOCKING).remove(chunkKey);
         chunkKey = new ChunkCacheKey(indexName, filename, ++i);
      } while (removed != null);
      cache.startBatch();
      cache.withFlags(Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_LOCKING).remove(readLockKey);
      FileCacheKey key = new FileCacheKey(indexName, filename);
      cache.withFlags(Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_LOCKING).remove(key);
      cache.endBatch(true);
   }

   /**
    * Acquires a readlock on all chunks for this file, to make sure chunks are not deleted while
    * iterating on the group. This is needed to avoid an eager lock on all elements.
    * 
    * @see #releaseReadLock(FileReadLockKey, AdvancedCache)
    */
   private void aquireReadLock() throws FileNotFoundException {
      // spinning as we currently don't mandate transactions, so no proper lock support is available
      boolean done = false;
      while (done == false) {
         Object lockValue = cache.withFlags(Flag.SKIP_LOCKING).get(readLockKey);
         if (lockValue == null)
            return; // no special locking for some core files
         int refCount = (Integer) lockValue;
         if (refCount == 0) {
            // too late: in case refCount==0 the delete process already triggered chunk deletion.
            // safest reaction is to tell this file doesn't exist anymore.
            throw new FileNotFoundException("segment file was deleted");
         }
         Integer newValue = Integer.valueOf(refCount + 1);
         done = cache.replace(readLockKey, refCount, newValue);
      }
   }

   @Override
   public byte readByte() throws IOException {
      if (bufferPosition >= currentBufferSize) {
         nextChunk();
         bufferPosition = 0;
      }
      return buffer[bufferPosition++];
    }
   
   @Override
   public void readBytes(byte[] b, int offset, int len) throws IOException {
      int bytesToRead = len;
      if (buffer == null) {
         nextChunk();
      }
      while (bytesToRead > 0) {
         int bytesToCopy = Math.min(currentBufferSize - bufferPosition, bytesToRead);
         System.arraycopy(buffer, bufferPosition, b, offset, bytesToCopy);
         offset += bytesToCopy;
         bytesToRead -= bytesToCopy;
         bufferPosition += bytesToCopy;
         if (bufferPosition >= currentBufferSize && bytesToRead > 0) {
            nextChunk();
            bufferPosition = 0;
         }
      }
   }

   @Override
   public void close() throws IOException {
      currentBufferSize = 0;
      bufferPosition = 0;
      currentLoadedChunk = -1;
      buffer = null;
      if (isClone) return;
      releaseReadLock();
      if (log.isDebugEnabled()) {
         log.debug("Closed IndexInput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
      }
   }

   public long getFilePointer() {
      return ((long) currentLoadedChunk) * chunkSize + bufferPosition;
   }

   @Override
   public void seek(long pos) throws IOException {
      bufferPosition = (int) (pos % chunkSize);
      int targetChunk = (int) (pos / chunkSize);
      if (targetChunk != currentLoadedChunk) {
         currentLoadedChunk = targetChunk;
         setBufferToCurrentChunkIfPossible();
      }
   }
   
   private void nextChunk() throws IOException {
      currentLoadedChunk++;
      setBufferToCurrentChunk();
   }

   private void setBufferToCurrentChunk() throws IOException {
      CacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), currentLoadedChunk);
      buffer = (byte[]) cache.withFlags(Flag.SKIP_LOCKING).get(key);
      if (buffer == null) {
         throw new IOException("Chunk value could not be found for key " + key);
      }
      currentBufferSize = buffer.length;
   }
   
   // Lucene might try seek(pos) using an illegal pos value
   // RAMDirectory teaches to position the cursor to the end of previous chunk in this case
   private void setBufferToCurrentChunkIfPossible() throws IOException {
      CacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), currentLoadedChunk);
      buffer = (byte[]) cache.withFlags(Flag.SKIP_LOCKING).get(key);
      if (buffer == null) {
         currentLoadedChunk--;
         bufferPosition = chunkSize;
      }
      else {
         currentBufferSize = buffer.length;
      }
   }

   @Override
   public long length() {
      return file.getSize();
   }
   
   @Override
   public Object clone() {
      InfinispanIndexInput clone = (InfinispanIndexInput)super.clone();
      // reference counting doesn't work properly: need to use isClone
      // as in other Directory implementations. Apparently not all clones
      // are cleaned up, but the original is (especially .tis files)
      clone.isClone = true; 
      return clone;
    }

}