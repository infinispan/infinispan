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

import org.apache.lucene.store.IndexOutput;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Responsible for writing to a <code>Directory</code>
 * 
 * @since 4.0
 * @author Sanne Grinovero
 * @author Lukasz Moren
 * @author Davide Di Somma
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.IndexInput
 */
@SuppressWarnings("unchecked")
public final class InfinispanIndexOutput extends IndexOutput {

   private static final Log log = LogFactory.getLog(InfinispanIndexOutput.class);
   private static final boolean trace = log.isTraceEnabled();

   private final int bufferSize;
   private final Cache<ChunkCacheKey, Object> chunksCache;
   private final Cache<ChunkCacheKey, Object> chunksCacheForStorage;
   private final AdvancedCache<FileCacheKey, FileMetadata> metadataCache;
   private final FileMetadata file;
   private final FileCacheKey fileKey;
   private final FileListOperations fileOps;

   private byte[] buffer;
   
   /**
    * First bytes are rewritten at close - we can minimize locking needs by flushing the first chunk
    * only once (as final operation: so always keep a pointer to the first buffer)
    */
   private byte[] firstChunkBuffer;
   private int positionInBuffer = 0;
   private long filePosition = 0;
   private int currentChunkNumber = 0;

   public InfinispanIndexOutput(final AdvancedCache<?, ?> metadataCache, final AdvancedCache<?, ?> chunksCache, final FileCacheKey fileKey, final int bufferSize, final FileListOperations fileList) {
      this.metadataCache = (AdvancedCache<FileCacheKey, FileMetadata>) metadataCache;
      this.chunksCache = (Cache<ChunkCacheKey, Object>) chunksCache;
      this.chunksCacheForStorage = (Cache<ChunkCacheKey, Object>) chunksCache.withFlags(Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_CACHE_LOAD);
      this.fileKey = fileKey;
      this.bufferSize = bufferSize;
      this.fileOps = fileList;
      this.buffer = new byte[this.bufferSize];
      this.firstChunkBuffer = buffer;
      this.file = new FileMetadata(bufferSize);
      if (trace) {
         log.tracef("Opened new IndexOutput for file:%s in index: %s", fileKey.getFileName(), fileKey.getIndexName());
      }
   }
   
   private byte[] getChunkById(FileCacheKey fileKey, int chunkNumber, int bufferSize, FileMetadata file) {
      if (file.getNumberOfChunks() <= chunkNumber) {
         return new byte[bufferSize];
      }
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), chunkNumber);
      byte[] readBuffer = (byte[]) chunksCache.get(key);
      if (readBuffer==null) {
         return new byte[bufferSize];
      }
      else if (readBuffer.length == bufferSize) {
         return readBuffer;
      }
      else {
         byte[] newBuffer = new byte[bufferSize];
         System.arraycopy(readBuffer, 0, newBuffer, 0, readBuffer.length);
         return newBuffer;
      }
   }

   private static int getPositionInBuffer(final long pos, final int bufferSize) {
      return (int) (pos % bufferSize);
   }

   private static int getChunkNumberFromPosition(final long pos, final int bufferSize) {
      return (int) ((pos) / (bufferSize));
   }

   private void newChunk() {
      storeCurrentBuffer(false);// save data first
      currentChunkNumber++;
      // check if we have to create new chunk, or get already existing in cache for modification
      buffer = getChunkById(fileKey, currentChunkNumber, bufferSize, file);
      positionInBuffer = 0;
   }

   @Override
   public final void writeByte(final byte b) {
      if (isNewChunkNeeded()) {
         newChunk();
      }
      buffer[positionInBuffer++] = b;
      filePosition++;
   }

   @Override
   public final void writeBytes(final byte[] b, final int offset, final int length) {
      int writtenBytes = 0;
      while (writtenBytes < length) {
         if (isNewChunkNeeded()) {
            newChunk();
         }
         int pieceLength = Math.min(bufferSize - positionInBuffer, length - writtenBytes);
         System.arraycopy(b, offset + writtenBytes, buffer, positionInBuffer, pieceLength);
         positionInBuffer += pieceLength;
         filePosition += pieceLength;
         writtenBytes += pieceLength;
      }
   }

   private boolean isNewChunkNeeded() {
      return (positionInBuffer == buffer.length);
   }

   @Override
   public void flush() {
      storeCurrentBuffer(false);
   }

   protected void storeCurrentBuffer(final boolean isClose) {
      if (currentChunkNumber == 0 && ! isClose) {
         //we don't store the first chunk until the close operation: this way
         //we guarantee each chunk is written only once an minimize locking needs.
         return;
      }
      // size changed, apply change to file header
      resizeFileIfNeeded();
      byte[] bufferToFlush = buffer;
      boolean writingOnLastChunk = isWritingOnLastChunk();
      if (writingOnLastChunk) {
         int newBufferSize = (int) (file.getSize() % bufferSize);
         if (newBufferSize != 0) {
            bufferToFlush = new byte[newBufferSize];
            System.arraycopy(buffer, 0, bufferToFlush, 0, newBufferSize);
         }
      }
      
      // add chunk to cache
      if ( ! writingOnLastChunk || this.positionInBuffer != 0) {
         // store the current chunk
         storeBufferAsChunk(bufferToFlush, currentChunkNumber);
      }
   }

   /**
    * @param bufferToFlush
    * @param chunkNumber
    */
   private void storeBufferAsChunk(final byte[] bufferToFlush, final int chunkNumber) {
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), chunkNumber);
      if (trace) log.tracef("Storing segment chunk: %s", key);
      chunksCacheForStorage.put(key, bufferToFlush);
   }

   private void resizeFileIfNeeded() {
      if (file.getSize() < filePosition) {
         file.setSize(filePosition);
      }
   }

   @Override
   public void close() {
      final boolean microbatch = chunksCache.startBatch();
      if (currentChunkNumber==0) {
         //store current chunk, possibly resizing it
         storeCurrentBuffer(true);
      }
      else {
         //no need to resize first chunk, just store it:
         storeBufferAsChunk(this.firstChunkBuffer, 0);
         storeCurrentBuffer(true);
      }
      buffer = null;
      firstChunkBuffer = null;
      // override existing file header with updated accesstime
      file.touch();
      metadataCache.withFlags(Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_CACHE_LOAD).put(fileKey, file);
      fileOps.addFileName(this.fileKey.getFileName());
      if (microbatch) chunksCache.endBatch(true);
      if (trace) {
         log.tracef("Closed IndexOutput for %s", fileKey);
      }
   }

   @Override
   public long getFilePointer() {
      return filePosition;
   }

   @Override
   public void seek(final long pos) throws IOException {
      final int requestedChunkNumber = getChunkNumberFromPosition(pos, bufferSize);
      if (pos > file.getSize()) {
         resizeFileIfNeeded();
         if (pos > file.getSize()) // check again, might be fixed by the resize
            throw new IOException(fileKey.getFileName() + ": seeking past end of file");
      }
      if (requestedChunkNumber != currentChunkNumber) {
         storeCurrentBuffer(false);
         if (requestedChunkNumber != 0) {
            buffer = getChunkById(fileKey, requestedChunkNumber, bufferSize, file);
         }
         else {
            buffer = firstChunkBuffer;
         }
         currentChunkNumber = requestedChunkNumber;
      }
      positionInBuffer = getPositionInBuffer(pos, bufferSize);
      filePosition = pos;
   }

   @Override
   public long length() {
      return file.getSize();
   }
   
   private boolean isWritingOnLastChunk() {
      final int lastChunkNumber = file.getNumberOfChunks() - 1;
      return currentChunkNumber >= lastChunkNumber;
   }
   
}
