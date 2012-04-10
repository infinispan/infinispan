/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.lucene.readlocks.SegmentReadLocker;
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
@SuppressWarnings("unchecked")
final public class InfinispanIndexInput extends IndexInput {

   private static final Log log = LogFactory.getLog(InfinispanIndexInput.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<ChunkCacheKey, Object> chunksCache;
   private final FileCacheKey fileKey;
   private final int chunkSize;
   private final SegmentReadLocker readLocks;
   private final String filename;
   private final long fileLength;

   private int currentBufferSize;
   private byte[] buffer;
   private int bufferPosition;
   private int currentLoadedChunk = -1;

   private boolean isClone;

   public InfinispanIndexInput(final AdvancedCache<?, ?> chunksCache, final FileCacheKey fileKey, final FileMetadata fileMetadata, final SegmentReadLocker readLocks) {
      this.chunksCache = (Cache<ChunkCacheKey, Object>) chunksCache;
      this.fileKey = fileKey;
      this.chunkSize = fileMetadata.getBufferSize();
      this.fileLength = fileMetadata.getSize();
      this.readLocks = readLocks;
      this.filename = fileKey.getFileName();
      if (trace) {
         log.tracef("Opened new IndexInput for file:%s in index: %s", filename, fileKey.getIndexName());
      }
   }

   @Override
   public final byte readByte() throws IOException {
      if (bufferPosition >= currentBufferSize) {
         nextChunk();
         bufferPosition = 0;
      }
      return buffer[bufferPosition++];
    }
   
   @Override
   public final void readBytes(final byte[] b, int offset, int bytesToRead) throws IOException {
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
   public void close() {
      currentBufferSize = 0;
      bufferPosition = 0;
      currentLoadedChunk = -1;
      buffer = null;
      if (isClone) return;
      readLocks.deleteOrReleaseReadLock(filename);
      if (trace) {
         log.tracef("Closed IndexInput for file:%s in index: %s", filename, fileKey.getIndexName());
      }
   }

   public long getFilePointer() {
      return ((long) currentLoadedChunk) * chunkSize + bufferPosition;
   }

   @Override
   public void seek(final long pos) {
      bufferPosition = (int) (pos % chunkSize);
      final int targetChunk = (int) (pos / chunkSize);
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
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), filename, currentLoadedChunk);
      buffer = (byte[]) chunksCache.get(key);
      if (buffer == null) {
         throw new IOException("Read past EOF: Chunk value could not be found for key " + key);
      }
      currentBufferSize = buffer.length;
   }
   
   // Lucene might try seek(pos) using an illegal pos value
   // RAMDirectory teaches to position the cursor to the end of previous chunk in this case
   private void setBufferToCurrentChunkIfPossible() {
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), filename, currentLoadedChunk);
      buffer = (byte[]) chunksCache.get(key);
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
      return this.fileLength;
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