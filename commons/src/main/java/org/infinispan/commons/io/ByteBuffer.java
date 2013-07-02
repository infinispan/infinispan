package org.infinispan.commons.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * A byte buffer that exposes the internal byte array with minimal copying
 *
 * @author (various)
 * @since 4.0
 */
public class ByteBuffer {
   private final byte[] buf;
   private final int offset;
   private final int length;

   public ByteBuffer(byte[] buf, int offset, int length) {
      this.buf = buf;
      this.offset = offset;
      this.length = length;
   }

   public byte[] getBuf() {
      return buf;
   }

   public int getOffset() {
      return offset;
   }

   public int getLength() {
      return length;
   }

   public ByteBuffer copy() {
      byte[] new_buf = buf != null ? new byte[length] : null;
      int new_length = new_buf != null ? new_buf.length : 0;
      if (new_buf != null)
         System.arraycopy(buf, offset, new_buf, 0, length);
      return new ByteBuffer(new_buf, 0, new_length);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(length).append(" bytes");
      if (offset > 0)
         sb.append(" (offset=").append(offset).append(")");
      return sb.toString();
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
