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
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Deal with input-output operations on Infinispan based Directory
 * 
 * @since 4.0
 * @author Lukasz Moren
 * @see org.apache.lucene.store.Directory
 * @see org.apache.lucene.store.IndexInput
 * @see org.apache.lucene.store.IndexOutput
 */
public class InfinispanIndexIO {

   // used as default chunk size if not provided in conf
   // each Lucene index is splitted into parts with default size defined here
   public final static int DEFAULT_BUFFER_SIZE = 16 * 1024;

   private static byte[] getChunkFromPosition(Map<CacheKey, Object> cache, FileCacheKey fileKey, int pos, int bufferSize) {
      CacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), getChunkNumberFromPosition(pos,
               bufferSize));
      return (byte[]) cache.get(key);
   }

   private static int getPositionInBuffer(int pos, int bufferSize) {
      return (pos % bufferSize);
   }

   private static int getChunkNumberFromPosition(int pos, int bufferSize) {
      return ((pos) / (bufferSize));
   }

   /**
    * Responsible for writing into <code>Directory</code>
    */
   public static class InfinispanIndexInput extends IndexInput {

      private static final Log log = LogFactory.getLog(InfinispanIndexInput.class);

      private final int bufferSize;

      private Cache<CacheKey, Object> cache;
      private ConcurrentHashMap<CacheKey, Object> localCache = new ConcurrentHashMap<CacheKey, Object>();
      private FileMetadata file;
      private FileCacheKey fileKey;
      private byte[] buffer;
      private int bufferPosition = 0;
      private int filePosition = 0;

      public InfinispanIndexInput(Cache<CacheKey, Object> cache, FileCacheKey fileKey) throws IOException {
         this(cache, fileKey, InfinispanIndexIO.DEFAULT_BUFFER_SIZE);
      }

      public InfinispanIndexInput(Cache<CacheKey, Object> cache, FileCacheKey fileKey, int bufferSize) throws IOException {
         this.cache = cache;
         this.fileKey = fileKey;
         this.bufferSize = bufferSize;
         buffer = new byte[this.bufferSize];

         // get file header from file
         this.file = (FileMetadata) cache.get(fileKey);

         if (file == null) {
            throw new IOException("File [ " + fileKey.getFileName() + " ] for index [ " + fileKey.getIndexName()
                     + " ] was not found");
         }

         // get records to local cache
         int i = 0;
         Object fileChunk;
         ChunkCacheKey chunkKey = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), i);
         while ((fileChunk = cache.get(chunkKey)) != null) {
            localCache.put(chunkKey, fileChunk);
            chunkKey = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), ++i);
         }

         if (log.isDebugEnabled()) {
            log.debug("Opened new IndexInput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
         }
      }

      private byte[] getChunkFromPosition(Cache<CacheKey, Object> cache, FileCacheKey fileKey, int pos, int bufferSize) {
         Object object = InfinispanIndexIO.getChunkFromPosition(cache, fileKey, pos, bufferSize);
         if (object == null) {
            object = InfinispanIndexIO.getChunkFromPosition(localCache, fileKey, pos, bufferSize);
         }
         return (byte[]) object;
      }

      public byte readByte() throws IOException {
         if (file == null) {
            throw new IOException("File " + fileKey + " does not exist");
         }

         if (filePosition == 0 && file.getSize() == 0) {
            if (log.isTraceEnabled()) {
               log.trace("pointer and file sizes are both 0; returning -1");
            }
            return -1;
         }

         buffer = getChunkFromPosition(cache, fileKey, filePosition, bufferSize);
         if (buffer == null) {
            throw new IOException("Chunk id = [ " + getChunkNumberFromPosition(filePosition, bufferSize)
                     + " ] does not exist for file [ " + fileKey.getFileName() + " ] for index [ "
                     + fileKey.getIndexName() + " ]");
         }

         bufferPosition = getPositionInBuffer(filePosition++, bufferSize);
         return buffer[bufferPosition];
      }

      public void readBytes(byte[] b, int offset, int len) throws IOException {

         if (file == null) {
            throw new IOException("(null): File does not exist");
         }

         if (filePosition == 0 && file.getSize() == 0) {
            if (log.isTraceEnabled()) {
               log.trace("pointer and file sizes are both 0; returning -1");
            }
         }

         int bytesToRead = len;
         while (bytesToRead > 0) {
            buffer = getChunkFromPosition(cache, fileKey, filePosition, bufferSize);
            if (buffer == null) {
               throw new IOException("Chunk id = [ " + getChunkNumberFromPosition(filePosition, bufferSize)
                        + " ] does not exist for file [ " + fileKey.getFileName() + " ] for index [ "
                        + fileKey.getIndexName() + " ], file position: [ " + filePosition + " ], file size: [ "
                        + file.getSize() + " ]");
            }
            bufferPosition = getPositionInBuffer(filePosition, bufferSize);
            int bytesToCopy = Math.min(buffer.length - bufferPosition, bytesToRead);
            System.arraycopy(buffer, bufferPosition, b, offset, bytesToCopy);
            offset += bytesToCopy;
            bytesToRead -= bytesToCopy;
            filePosition += bytesToCopy;
         }
      }

      public void close() throws IOException {
         filePosition = 0;
         bufferPosition = 0;
         buffer = null;
         localCache = null;
         if (log.isDebugEnabled()) {
            log.debug("Closed IndexInput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
         }
      }

      public long getFilePointer() {
         return filePosition;
      }

      public void seek(long pos) throws IOException {
         filePosition = (int) pos;
      }

      public long length() {
         return file.getSize();
      }
   }

   /**
    * Responsible for reading from <code>Directory</code>
    */
   public static class InfinispanIndexOutput extends IndexOutput {

      private static final Log log = LogFactory.getLog(InfinispanIndexOutput.class);

      private final int bufferSize;

      private Cache<CacheKey, Object> cache;
      private FileMetadata file;
      private FileCacheKey fileKey;

      private byte[] buffer;
      private int bufferPosition = 0;
      private int filePosition = 0;
      private int chunkNumber;

      public InfinispanIndexOutput(Cache<CacheKey, Object> cache, FileCacheKey fileKey) throws IOException {
         this(cache, fileKey, InfinispanIndexIO.DEFAULT_BUFFER_SIZE);
      }

      public InfinispanIndexOutput(Cache<CacheKey, Object> cache, FileCacheKey fileKey, int bufferSize) throws IOException {
         this.cache = cache;
         this.fileKey = fileKey;
         this.bufferSize = bufferSize;

         buffer = new byte[this.bufferSize];

         this.file = (FileMetadata) cache.get(fileKey);
         if (log.isDebugEnabled()) {
            log.debug("Opened new IndexOutput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
         }
      }

      private void newChunk() throws IOException {
         flush();// save data first

         // check if we have to create new chunk, or get already existing in cache for modification
         if ((buffer = getChunkFromPosition(cache, fileKey, filePosition, bufferSize)) == null) {
            buffer = new byte[bufferSize];
         }
         bufferPosition = 0;
      }

      public void writeByte(byte b) throws IOException {
         if (isNewChunkNeeded()) {
            newChunk();
         }
         buffer[bufferPosition++] = b;
         filePosition++;
      }

      public void writeBytes(byte[] b, int offset, int length) throws IOException {

         int writedBytes = 0;
         while (writedBytes < length) {
            int pieceLength = Math.min(buffer.length - bufferPosition, length - writedBytes);
            System.arraycopy(b, offset + writedBytes, buffer, bufferPosition, pieceLength);
            bufferPosition += pieceLength;
            filePosition += pieceLength;
            writedBytes += pieceLength;
            if (isNewChunkNeeded()) {
               newChunk();
            }
         }
      }

      private boolean isNewChunkNeeded() {
         return (bufferPosition == buffer.length);
      }

      public void flush() throws IOException {
         // select right chunkNumber
         chunkNumber = getChunkNumberFromPosition(filePosition - 1, bufferSize);
         // and create distinct key for it
         ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), chunkNumber);
         // size changed, apply change to file header
         setFileLength();
         // add chunk to cache
         cache.put(key, buffer);
         // override existing file header with new size and last time acess
         cache.put(fileKey, file);
      }

      public void close() throws IOException {
         flush();
         bufferPosition = 0;
         filePosition = 0;
         buffer = null;
         if (log.isDebugEnabled()) {
            log.debug("Closed IndexOutput for file:{0} in index: {1}", fileKey.getFileName(), fileKey.getIndexName());
         }
      }

      public long getFilePointer() {
         return filePosition;
      }

      public void seek(long pos) throws IOException {
         flush();

         if (pos > file.getSize()) {
            throw new IOException(fileKey.getFileName() + ": seeking past of the file");
         }

         buffer = getChunkFromPosition(cache, fileKey, (int) pos, bufferSize);
         bufferPosition = getPositionInBuffer((int) pos, bufferSize);
         filePosition = (int) pos;
      }

      public long length() throws IOException {
         return file.getSize();
      }

      protected void setFileLength() {
         file.touch();
         if (file.getSize() < filePosition) {
            file.setSize(filePosition);
         }
      }
   }
}
