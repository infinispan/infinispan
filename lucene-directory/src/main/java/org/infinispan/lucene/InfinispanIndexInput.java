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

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.infinispan.AdvancedCache;
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

   private int currentBufferSize;
   private byte[] buffer;
   private int bufferPosition;
   private int currentLoadedChunk = -1;

   public InfinispanIndexInput(AdvancedCache<CacheKey, Object> cache, FileCacheKey fileKey, int chunkSize) throws IOException {
      this.cache = cache;
      this.fileKey = fileKey;
      this.chunkSize = chunkSize;

      // get file header from file
      this.file = (FileMetadata) cache.get(fileKey);

      if (file == null) {
         throw new IOException("Error loading medatada for index file: " + fileKey);
      }

      if (log.isDebugEnabled()) {
         log.debug("Opened new IndexInput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
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
      if (log.isDebugEnabled()) {
         log.debug("Closed IndexInput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
      }
   }

   public long getFilePointer() {
      return ((long)currentLoadedChunk) * chunkSize + bufferPosition;
   }

   @Override
   public void seek(long pos) throws IOException {
      bufferPosition = (int)( pos % chunkSize );
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
      buffer = (byte[]) cache.get(key);
      if (buffer == null) {
         throw new IOException("Chunk value could not be found for key " + key);
      }
      currentBufferSize = buffer.length;
   }
   
   // Lucene might try seek(pos) using an illegal pos value
   // RAMDirectory teaches to position the cursor to the end of previous chunk in this case
   private void setBufferToCurrentChunkIfPossible() throws IOException {
      CacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), currentLoadedChunk);
      buffer = (byte[]) cache.get(key);
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
   
}