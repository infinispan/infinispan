package org.infinispan.commons.io;

import java.io.OutputStream;
import java.util.Arrays;

import org.infinispan.commons.util.Util;

import net.jcip.annotations.NotThreadSafe;

/**
 * ByteArrayOutputStream alternative exposing the internal buffer. Using this, callers don't need to call toByteArray()
 * which copies the internal buffer.
 *
 * <p>This class doubles the size until the internal buffer reaches a
 * configurable max size (default is 4MB), after which it begins growing the buffer in 25% increments.
 * This is intended to help prevent an OutOfMemoryError during a resize of a large buffer. </p>
 * <p> A version of this class was originally created by Bela Ban as part of the JGroups library. </p>
 * <p>This class is not threadsafe as it will not support concurrent readers and writers. <p/>
 *
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @author Dan Berindei
 * @since 13.0
 */
@NotThreadSafe
public final class LazyByteArrayOutputStream extends OutputStream {
   public static final int DEFAULT_SIZE = 32;
   /**
    * Default buffer size after which if more buffer capacity is needed the buffer will grow by 25% rather than 100%
    */
   public static final int DEFAULT_DOUBLING_SIZE = 4 * 1024 * 1024; // 4MB

   private byte[] buf;
   private int count;

   private int maxDoublingSize = DEFAULT_DOUBLING_SIZE;

   public LazyByteArrayOutputStream() {
      buf = null;
   }

   public LazyByteArrayOutputStream(int size) {
      buf = new byte[size];
   }

   /**
    * Creates a new byte array output stream, with a buffer capacity of the specified size, in bytes.
    *
    * @param size            the initial size.
    * @param maxDoublingSize the buffer size, after which if more capacity is needed the buffer will grow by 25% rather
    *                        than 100%
    * @throws IllegalArgumentException if size is negative.
    */
   public LazyByteArrayOutputStream(int size, int maxDoublingSize) {
      this(size);
      this.maxDoublingSize = maxDoublingSize;
   }

   /**
    * Gets the internal buffer array. Note that the length of this array will almost certainly be longer than the data
    * written to it; call <code>size()</code> to get the number of bytes of actual data.
    */
   public byte[] getRawBuffer() {
      if (buf == null)
         return Util.EMPTY_BYTE_ARRAY;
      return buf;
   }

   /**
    * Gets a buffer that is trimmed so that there are no excess bytes. If the current count does not match the
    * underlying buffer than a new one is created with the written bytes.
    *
    * @return a byte[] that contains all the bytes written to this stream
    */
   public byte[] getTrimmedBuffer() {
      if (buf == null) {
         return Util.EMPTY_BYTE_ARRAY;
      }
      if (buf.length == count) {
         return buf;
      }
      return Arrays.copyOf(this.buf, this.count);
   }

   @Override
   public void write(byte[] b, int off, int len) {
      if ((off < 0) || (off > b.length) || (len < 0) ||
            ((off + len) > b.length) || ((off + len) < 0)) {
         throw new IndexOutOfBoundsException();
      } else if (len == 0) {
         return;
      }

      int newcount = count + len;
      ensureCapacity(newcount);

      System.arraycopy(b, off, buf, count, len);
      count = newcount;
   }

   @Override
   public void write(int b) {
      int newcount = count + 1;
      ensureCapacity(newcount);
      buf[count] = (byte) b;
      count = newcount;
   }

   private void ensureCapacity(int newcount) {
      if (buf == null) {
         // Pretend we have half the default size so it's doubled
         buf = new byte[Math.max(DEFAULT_SIZE, newcount)];
      } else if (newcount > buf.length) {
         byte[] newbuf = new byte[getNewBufferSize(buf.length, newcount)];
         System.arraycopy(buf, 0, newbuf, 0, count);
         buf = newbuf;
      }
   }

   /**
    * Gets the highest internal buffer size after which if more capacity is needed the buffer will grow in 25%
    * increments rather than 100%.
    */
   public final int getMaxDoublingSize() {
      return maxDoublingSize;
   }

   /**
    * Gets the number of bytes to which the internal buffer should be resized.
    *
    * @param curSize    the current number of bytes
    * @param minNewSize the minimum number of bytes required
    * @return the size to which the internal buffer should be resized
    */
   public int getNewBufferSize(int curSize, int minNewSize) {
      if (curSize <= maxDoublingSize)
         return Math.max(curSize << 1, minNewSize);
      else
         return Math.max(curSize + (curSize >> 2), minNewSize);
   }

   /**
    * Overriden only to avoid unneeded synchronization
    */
   public int size() {
      return count;
   }
}
