package org.infinispan.commons.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.infinispan.commons.util.Util;

/**
 * A byte buffer that exposes the internal byte array with minimal copying
 *
 * @author (various)
 * @since 4.0
 */
public class ByteBufferImpl implements ByteBuffer {

   public static final ByteBufferImpl EMPTY_INSTANCE = new ByteBufferImpl(Util.EMPTY_BYTE_ARRAY, 0, 0);

   private final byte[] buf;
   private final int offset;
   private final int length;

   public static ByteBufferImpl create(byte[] array) {
      return array == null || array.length == 0 ? EMPTY_INSTANCE : new ByteBufferImpl(array, 0, array.length);
   }

   public static ByteBufferImpl create(byte[] array, int offset, int length) {
      if (array == null || length == 0 || array.length == 0) {
         return EMPTY_INSTANCE;
      }
      if (array.length < offset + length)
         throw new IllegalArgumentException("Incorrect arguments: array.length" + array.length + ", offset=" + offset + ", length=" + length);
      return new ByteBufferImpl(array, offset, length);
   }

   public static ByteBufferImpl create(java.nio.ByteBuffer javaBuffer) {
      int remaining = javaBuffer.remaining();
      if (remaining == 0) {
         return EMPTY_INSTANCE;
      }
      return new ByteBufferImpl(javaBuffer.array(), javaBuffer.arrayOffset(), remaining);
   }

   public static ByteBufferImpl create(byte b) {
      return new ByteBufferImpl(new byte[] {b}, 0, 1);
   }

   private ByteBufferImpl(byte[] buf, int offset, int length) {
      this.buf = buf;
      this.offset = offset;
      this.length = length;
   }

   @Override
   public byte[] getBuf() {
      return buf;
   }

   @Override
   public int getOffset() {
      return offset;
   }

   @Override
   public int getLength() {
      return length;
   }

   @Override
   public ByteBufferImpl copy() {
      if (this == EMPTY_INSTANCE)
         return this;
      byte[] new_buf = new byte[length];
      System.arraycopy(buf, offset, new_buf, 0, length);
      return new ByteBufferImpl(new_buf, 0, length);
   }

   @Override
   public String toString() {
      return String.format("ByteBufferImpl{length=%d, offset=%d, bytes=%s}", length, offset, Util.hexDump(buf, offset, length));
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ByteBufferImpl)) return false;

      ByteBufferImpl that = (ByteBufferImpl) o;

      if (length != that.length)
         return false;

      for (int i = 0; i < length; i++)
         if (buf[offset + i] != that.buf[that.offset + i])
            return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = 1;
      for (int i = 0; i < length; i++) {
         result = 31 * result + buf[offset + i];
      }
      return result;
   }

   /**
    * @return an input stream for the bytes in the buffer
    */
   public InputStream getStream() {
      return new ByteArrayInputStream(getBuf(), getOffset(), getLength());
   }

   public java.nio.ByteBuffer toJDKByteBuffer() {
      return java.nio.ByteBuffer.wrap(buf, offset, length);
   }
}
