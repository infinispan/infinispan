/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.io;

/**
 * A byte stream that can be written to and expanded on the fly, not dissimilar to {@link ExposedByteArrayOutputStream}
 * but with the benefit of not having to allocate unnecessary byte arrays byt not extending {@link java.io.ByteArrayOutputStream}.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ExpandableMarshalledValueByteStream extends MarshalledValueByteStream {
   /**
    * The buffer where data is stored.
    */
   private byte buf[];

   /**
    * The number of valid bytes in the buffer.
    */
   private int count;

   /**
    * Default buffer size after which if more buffer capacity is needed the buffer will grow by 25% rather than 100%
    */
   public static final int DEFAULT_DOUBLING_SIZE = 4 * 1024 * 1024; // 4MB

   private int maxDoublingSize = DEFAULT_DOUBLING_SIZE;

   public ExpandableMarshalledValueByteStream() {
      this(32);
   }

   public ExpandableMarshalledValueByteStream(int size) {
      if (size < 0) {
         throw new IllegalArgumentException("Negative initial size: "
                                                  + size);
      }
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
   public ExpandableMarshalledValueByteStream(int size, int maxDoublingSize) {
      this(size);
      this.maxDoublingSize = maxDoublingSize;
   }

   /**
    * Gets the internal buffer array. Note that the length of this array will almost certainly be longer than the data
    * written to it; call <code>size()</code> to get the number of bytes of actual data.
    */
   @Override
   public final byte[] getRaw() {
      return buf;
   }

   public final void set(byte[] b) {
      this.buf = b;
      this.count = b.length;
   }

   @Override
   public final void write(byte[] b, int off, int len) {
      if ((off < 0) || (off > b.length) || (len < 0) ||
            ((off + len) > b.length) || ((off + len) < 0)) {
         throw new IndexOutOfBoundsException();
      } else if (len == 0) {
         return;
      }

      int newcount = count + len;
      if (newcount > buf.length) {
         byte newbuf[] = new byte[getNewBufferSize(buf.length, newcount)];
         System.arraycopy(buf, 0, newbuf, 0, count);
         buf = newbuf;
      }

      System.arraycopy(b, off, buf, count, len);
      count = newcount;
   }

   @Override
   public final void write(int b) {
      int newcount = count + 1;
      if (newcount > buf.length) {
         byte newbuf[] = new byte[getNewBufferSize(buf.length, newcount)];
         System.arraycopy(buf, 0, newbuf, 0, count);
         buf = newbuf;
      }
      buf[count] = (byte) b;
      count = newcount;
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
   public final int getNewBufferSize(int curSize, int minNewSize) {
      if (curSize <= maxDoublingSize)
         return Math.max(curSize << 1, minNewSize);
      else
         return Math.max(curSize + (curSize >> 2), minNewSize);
   }

   /**
    * Overriden only to avoid unneeded synchronization
    */
   @Override
   public final int size() {
      return count;
   }

   @Override
   public boolean equals(Object thatObject) {
      if (thatObject instanceof MarshalledValueByteStream) {
         MarshalledValueByteStream that = (MarshalledValueByteStream) thatObject;
         if (this == that) return true;
         byte[] thoseBytes = that.getRaw();
         if (this.buf == thoseBytes) return true;
         if (this.count != that.size()) return false;
         for (int i = 0; i < count; i++) {
            if (this.buf[i] != thoseBytes[i]) return false;
         }
         return true;
      } else {
         return false;
      }
   }
}
