package org.infinispan.lucene.impl;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

/**
 * Wraps a buffer to expose only a slice of it.
 * The buffer is not copied to avoid copy operations as the slice is expected to have a lifespan shorter than the buffer itself,
 * so there would be no benefit in having a smaller copy in heap.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
final class SlicedBufferIndexInput extends IndexInput {

   private final byte[] buffer;
   private final int offset;
   private final int length;
   private final int hardLimit;
   private int bufferPosition;

   protected SlicedBufferIndexInput(String resourceDescription, byte[] buffer, long offset, long length) {
      super(resourceDescription);
      this.buffer = buffer;
      this.offset = toInt(offset);
      this.length = toInt(length);
      this.hardLimit = this.offset + this.length;
      this.bufferPosition = this.offset;
      if (hardLimit > buffer.length) {
         throw new IllegalArgumentException("offset or length too large for the size of this buffer");
      }
   }

   @Override
   public void close() throws IOException {
      //no-op
   }

   @Override
   public long getFilePointer() {
      return bufferPosition - offset;
   }

   @Override
   public void seek(long pos) throws IOException {
      //Lucene might use positions larger than length(), in
      //this case you have to position the pointer to eof.
      bufferPosition = Math.min(hardLimit, ((int)pos) + offset);
   }

   @Override
   public long length() {
      return this.length;
   }

   public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new SlicedBufferIndexInput(sliceDescription, buffer, offset + this.offset, length);
   }

   @Override
   public byte readByte() throws IOException {
      if (bufferPosition >= hardLimit) {
         throw new IOException("Read past EOF");
      }
      return buffer[bufferPosition++];
   }

   @Override
   public void readBytes(byte[] b, int offset, int len) throws IOException {
      if (hardLimit - bufferPosition < len) {
         throw new IOException("Read past EOF");
      }
      System.arraycopy(buffer, bufferPosition, b, offset, len);
      bufferPosition+=len;
   }

   private static int toInt(final long n) {
      if ( n < 0 || n > Integer.MAX_VALUE) {
         throw new IllegalArgumentException("A SlicedBufferIndexInput can only be defined with offset and length which are positive and fit in an int");
      }
      return (int) n;
   }

}
