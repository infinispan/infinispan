package org.infinispan.commons.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.infinispan.commons.util.Util;

/**
 * A byte buffer that exposes the internal byte array with minimal copying. To be instantiated with {@link
 * ByteBufferFactory}.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface ByteBuffer {
   /**
    * Returns the underlying buffer.
    */
   byte[] getBuf();

   /**
    * Returns the offset within the underlying byte[] (as returned by {@link #getBuf()} owned by this buffer instance.
    */
   int getOffset();

   /**
    * Length bytes, starting from offset, within the underlying byte[] (as returned by {@link #getBuf()} are owned by
    * this buffer instance.
    */
   int getLength();

   /**
    * Returns a new byte[] instance of size {@link #getLength()} that contains all the bytes owned by this buffer.
    */
   ByteBuffer copy();

   /**
    * Returns a trimmed byte array.
    * <p>
    * The returned byte array should not be modified. A copy is not guaranteed.
    * <p>
    * It does not copy the byte array if {@link #getOffset()} is zero and {@link #getLength()} is equals to the
    * underlying byte array length.
    *
    * @return A trimmed byte array.
    */
   default byte[] trim() {
      if (getLength() == 0) {
         return Util.EMPTY_BYTE_ARRAY;
      }
      if (getOffset() == 0 && getBuf().length == getLength()) {
         // avoid copying it
         return getBuf();
      }
      byte[] trimBuf = new byte[getLength()];
      System.arraycopy(getBuf(), getOffset(), trimBuf, 0, getLength());
      return trimBuf;
   }

   /**
    * @return An {@link InputStream} for the content of this {@link ByteBuffer}.
    */
   default InputStream getStream() {
      return new ByteArrayInputStream(getBuf(), getOffset(), getLength());
   }
}
