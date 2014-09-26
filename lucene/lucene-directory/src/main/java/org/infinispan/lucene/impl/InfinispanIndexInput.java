package org.infinispan.lucene.impl;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.infinispan.Cache;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
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
public class InfinispanIndexInput extends IndexInput {


   private static final Log log = LogFactory.getLog(InfinispanIndexInput.class);
   private static final boolean trace = log.isTraceEnabled();

   protected boolean isClone;

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

   public InfinispanIndexInput(final IndexInputContext ctx) {
      super(ctx.fileKey.getFileName());
      this.chunksCache = ctx.chunksCache;
      this.fileKey = ctx.fileKey;
      this.chunkSize = ctx.fileMetadata.getBufferSize();
      this.fileLength = ctx.fileMetadata.getSize();
      this.readLocks = ctx.readLocks;
      this.filename = fileKey.getFileName();
      if (trace) {
         log.tracef("Opened new IndexInput for file:%s in index: %s", filename, fileKey.getIndexName());
      }
   }

   private InfinispanIndexInput(final String resourceDescription, final Cache<ChunkCacheKey, Object> chunksCache, FileCacheKey fileKey, int chunkSize, String filename, long fileLength) {
      super(resourceDescription);
      this.chunksCache = chunksCache;
      this.fileKey = fileKey;
      this.chunkSize = chunkSize;
      this.filename = filename;
      this.fileLength = fileLength;
      this.readLocks = null;//Lifecycle of this IndexInput is dependent on a parent IndexInput
      this.isClone = true;
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

   @Override
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
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), filename, currentLoadedChunk, chunkSize);
      buffer = (byte[]) chunksCache.get(key);
      if (buffer == null) {
         throw new IOException("Read past EOF: Chunk value could not be found for key " + key);
      }
      currentBufferSize = buffer.length;
   }

   // Lucene might try seek(pos) using an illegal pos value
   // RAMDirectory teaches to position the cursor to the end of previous chunk in this case
   private void setBufferToCurrentChunkIfPossible() {
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), filename, currentLoadedChunk, chunkSize);
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
   public InfinispanIndexInput clone() {
      InfinispanIndexInput clone = (InfinispanIndexInput)super.clone();
      // reference counting doesn't work properly: need to use isClone
      // as in other Directory implementations. Apparently not all clones
      // are cleaned up, but the original is (especially .tis files)
      clone.isClone = true;
      return clone;
   }

   public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new SlicingInfinispanIndexInput(sliceDescription, offset, length, copyAndReset());
   }

   InfinispanIndexInput copyAndReset() {
      return new InfinispanIndexInput(filename, chunksCache, fileKey, chunkSize, filename, fileLength);
   }

}
