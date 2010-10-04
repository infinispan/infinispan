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

import org.apache.lucene.store.IndexOutput;
import org.infinispan.AdvancedCache;
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
public class InfinispanIndexOutput extends IndexOutput {

   private static final Log log = LogFactory.getLog(InfinispanIndexOutput.class);

   private final int bufferSize;
   private final AdvancedCache chunksCache;
   private final AdvancedCache metadataCache;
   private final FileMetadata file;
   private final FileCacheKey fileKey;
   private final FileListOperations fileOps;
   private final boolean trace;

   private byte[] buffer;
   private int positionInBuffer = 0;
   private long filePosition = 0;
   private int currentChunkNumber = 0;
   private boolean needsAddingToFileList = true;

   public InfinispanIndexOutput(AdvancedCache metadataCache, AdvancedCache chunksCache, FileCacheKey fileKey, int bufferSize, FileListOperations fileList) throws IOException {
      this.metadataCache = metadataCache;
      this.chunksCache = chunksCache;
      this.fileKey = fileKey;
      this.bufferSize = bufferSize;
      this.fileOps = fileList;
      this.buffer = new byte[this.bufferSize];
      this.file = new FileMetadata();
      this.file.setBufferSize(bufferSize);
      trace = log.isTraceEnabled();
      if (trace) {
         log.trace("Opened new IndexOutput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
      }
   }
   
   private static byte[] getChunkById(AdvancedCache cache, FileCacheKey fileKey, int chunkNumber, int bufferSize) {
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), chunkNumber);
      byte[] readBuffer = (byte[]) cache.withFlags(Flag.SKIP_LOCKING).get(key);
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

   private static int getPositionInBuffer(long pos, int bufferSize) {
      return (int) (pos % bufferSize);
   }

   private static int getChunkNumberFromPosition(long pos, int bufferSize) {
      return (int) ((pos) / (bufferSize));
   }

   private void newChunk() throws IOException {
      storeCurrentBuffer();// save data first
      currentChunkNumber++;
      // check if we have to create new chunk, or get already existing in cache for modification
      buffer = getChunkById(chunksCache, fileKey, currentChunkNumber, bufferSize);
      positionInBuffer = 0;
   }

   @Override
   public final void writeByte(byte b) throws IOException {
      if (isNewChunkNeeded()) {
         newChunk();
      }
      buffer[positionInBuffer++] = b;
      filePosition++;
   }

   @Override
   public final void writeBytes(final byte[] b, final int offset, int length) throws IOException {
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
   public void flush() throws IOException {
      storeCurrentBuffer();
   }

   protected void storeCurrentBuffer() throws IOException {
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
      boolean microbatch = false;
      // add chunk to cache
      if ( ! writingOnLastChunk || this.positionInBuffer != 0) {
         if (chunksCache == metadataCache) {
            //as we do an operation on chunks and one on metadata, it's not useful to start a batch unless it's the same cache
            microbatch = chunksCache.startBatch();
         }
         // create key for the current chunk
         ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), currentChunkNumber);
         if (trace) log.trace("Storing segment chunk: " + key);
         chunksCache.withFlags(Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_LOCKING).put(key, bufferToFlush);
      }
      // override existing file header with new size and updated accesstime
      file.touch();
      metadataCache.withFlags(Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_LOCKING).put(fileKey, file);
      registerToFileListIfNeeded();
      if (microbatch) chunksCache.endBatch(true);
   }

   private void registerToFileListIfNeeded() {
      if (needsAddingToFileList) {
         fileOps.addFileName(this.fileKey.getFileName());
         needsAddingToFileList = false;
      }
   }

   private void resizeFileIfNeeded() {
      if (file.getSize() < filePosition) {
         file.setSize(filePosition);
      }
   }

   @Override
   public void close() throws IOException {
      storeCurrentBuffer();
      positionInBuffer = 0;
      filePosition = 0;
      buffer = null;
      if (trace) {
         log.trace("Closed IndexOutput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
      }
   }

   @Override
   public long getFilePointer() {
      return filePosition;
   }

   @Override
   public void seek(long pos) throws IOException {
      int requestedChunkNumber = getChunkNumberFromPosition(pos, bufferSize);
      if (pos > file.getSize()) {
         resizeFileIfNeeded();
         if (pos > file.getSize()) // check again, might be fixed by the resize
            throw new IOException(fileKey.getFileName() + ": seeking past end of file");
      }
      if (requestedChunkNumber != currentChunkNumber) {
         storeCurrentBuffer();
         buffer = getChunkById(chunksCache, fileKey, requestedChunkNumber, bufferSize);
         currentChunkNumber = requestedChunkNumber;
      }
      positionInBuffer = getPositionInBuffer(pos, bufferSize);
      filePosition = pos;
   }

   @Override
   public long length() throws IOException {
      return file.getSize();
   }
   
   private boolean isWritingOnLastChunk() {
      int lastChunkNumber = file.getNumberOfChunks() - 1;
      return currentChunkNumber >= lastChunkNumber;
   }
   
}
