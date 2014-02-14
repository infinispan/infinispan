package org.infinispan.lucene.impl;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.infinispan.lucene.ChunkCacheKey;

/**
 * SingleChunkIndexInput can be used instead of InfinispanIndexInput to read a segment
 * when it has a size small enough to fit in a single chunk.
 * In this quite common case for some segment types we
 * don't need the readLock to span multiple chunks, the pointer to the buffer is safe enough.
 * This leads to an extreme simple implementation.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public final class SingleChunkIndexInput extends IndexInput {

   private final byte[] buffer;
   private int bufferPosition;

   public SingleChunkIndexInput(final IndexInputContext iic) {
      super(iic.fileKey.getFileName());
      ChunkCacheKey key = new ChunkCacheKey(iic.fileKey.getIndexName(), iic.fileKey.getFileName(), 0, iic.fileMetadata.getBufferSize());
      byte[] b = (byte[]) iic.chunksCache.get(key);
      if (b == null) {
         buffer = new byte[0];
      }
      else {
         buffer = b;
      }
      bufferPosition = 0;
   }

   @Override
   public void close() {
      //nothing to do
   }

   @Override
   public long getFilePointer() {
      return bufferPosition;
   }

   @Override
   public long length() {
      return buffer.length;
   }

   @Override
   public byte readByte() throws IOException {
      if (bufferPosition >= buffer.length) {
         throw new IOException("Read past EOF");
      }
      return buffer[bufferPosition++];
   }

   @Override
   public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
      if (buffer.length - bufferPosition < len) {
         throw new IOException("Read past EOF");
      }
      System.arraycopy(buffer, bufferPosition, b, offset, len);
      bufferPosition+=len;
   }

   @Override
   public void seek(final long pos) {
      //Lucene might use positions larger than length(), in
      //this case you have to position the pointer to eof.
      bufferPosition = (int) Math.min(pos, buffer.length);
   }

}
