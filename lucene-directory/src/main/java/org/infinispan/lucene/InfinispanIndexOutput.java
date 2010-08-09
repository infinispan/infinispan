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
 * @author Lukasz Moren
 * @author Davide Di Somma
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.IndexInput
 */
public class InfinispanIndexOutput extends IndexOutput {

   private static final Log log = LogFactory.getLog(InfinispanIndexOutput.class);

   private final int bufferSize;

   private final AdvancedCache cache;
   private final FileMetadata file;
   private final FileCacheKey fileKey;

   private byte[] buffer;
   private int positionInBuffer = 0;
   private long filePosition = 0;
   private int currentChunkNumber = 0;

   public InfinispanIndexOutput(AdvancedCache cache, FileCacheKey fileKey, int bufferSize, FileMetadata fileMetadata) throws IOException {
      this.cache = cache;
      this.fileKey = fileKey;
      this.bufferSize = bufferSize;
      this.buffer = new byte[this.bufferSize];
      this.file = fileMetadata;
      if (log.isDebugEnabled()) {
         log.debug("Opened new IndexOutput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
      }
   }
   
   private static byte[] getChunkById(AdvancedCache cache, FileCacheKey fileKey, int chunkNumber, int bufferSize) {
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), chunkNumber);
      byte[] readBuffer = (byte[]) cache.withFlags(Flag.SKIP_LOCKING).get(key);
      if (readBuffer==null) {
         return new byte[bufferSize];
      }
      else if (readBuffer.length==bufferSize) {
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
      flush();// save data first
      currentChunkNumber++;
      // check if we have to create new chunk, or get already existing in cache for modification
      buffer = getChunkById(cache, fileKey, currentChunkNumber, bufferSize);
      positionInBuffer = 0;
   }

   @Override
   public void writeByte(byte b) throws IOException {
      if (isNewChunkNeeded()) {
         newChunk();
      }
      buffer[positionInBuffer++] = b;
      filePosition++;
   }

   @Override
   public void writeBytes(byte[] b, int offset, int length) throws IOException {
      int writtenBytes = 0;
      while (writtenBytes < length) {
         int pieceLength = Math.min(bufferSize - positionInBuffer, length - writtenBytes);
         System.arraycopy(b, offset + writtenBytes, buffer, positionInBuffer, pieceLength);
         positionInBuffer += pieceLength;
         filePosition += pieceLength;
         writtenBytes += pieceLength;
         if (isNewChunkNeeded()) {
            newChunk();
         }
      }
   }

   private boolean isNewChunkNeeded() {
      return (positionInBuffer == buffer.length);
   }

   @Override
   public void flush() throws IOException {
      // create key for the current chunk
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), currentChunkNumber);
      // size changed, apply change to file header
      file.touch();
      resizeFileIfNeeded();
      if (file.getSize() < filePosition) {
         file.setSize(filePosition);
      }
      byte[] bufferToFlush = buffer;
      if (isWritingOnLastChunk()) {
         int newBufferSize = (int) (file.getSize() % bufferSize);
         if (newBufferSize != 0) {
            bufferToFlush = new byte[newBufferSize];
            System.arraycopy(buffer, 0, bufferToFlush, 0, newBufferSize);
         }
      }
      cache.startBatch();
      // add chunk to cache
      cache.withFlags(Flag.SKIP_REMOTE_LOOKUP).put(key, bufferToFlush);
      // override existing file header with new size and last time access
      cache.withFlags(Flag.SKIP_REMOTE_LOOKUP).put(fileKey, file);
      cache.endBatch(true);
   }

   private void resizeFileIfNeeded() {
      if (file.getSize() < filePosition) {
         file.setSize(filePosition);
      }
   }

   @Override
   public void close() throws IOException {
      flush();
      positionInBuffer = 0;
      filePosition = 0;
      buffer = null;
      if (log.isDebugEnabled()) {
         log.debug("Closed IndexOutput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
      }
      // cache.compact(); //TODO investigate about this
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
            throw new IOException(fileKey.getFileName() + ": seeking past of the file");
      }
      if (requestedChunkNumber != currentChunkNumber) {
         flush();
         buffer = getChunkById(cache, fileKey, requestedChunkNumber, bufferSize);
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
      int lastChunkNumber = (int) (file.getSize() / bufferSize);
      return currentChunkNumber == lastChunkNumber;
   }
}
