package org.infinispan.commons.io;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ByteBuffer)) return false;

      ByteBuffer that = (ByteBuffer) o;

      return Arrays.equals(copy().getBuf(), that.copy().getBuf());
   }

   @Override
   public int hashCode() {
      return Arrays.hashCode(copy().getBuf());
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

   public void copy(byte[] result, int offset) {
      System.arraycopy(buf, offset, result, offset, length);
   }

   public static class Externalizer extends AbstractExternalizer<ByteBuffer> {

      private static final long serialVersionUID = -5291318076267612501L;

      @Override
      public void writeObject(ObjectOutput output, ByteBuffer b) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, b.length);
         output.write(b.buf, b.offset, b.length);
      }

      @Override
      public ByteBuffer readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int length = UnsignedNumeric.readUnsignedInt(input);
         byte[] data = new byte[length];
         input.read(data, 0, length);
         return new ByteBuffer(data, 0, length);
      }

      @Override
      public Integer getId() {
         return Ids.BYTE_BUFFER;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends ByteBuffer>> getTypeClasses() {
         return Util.<Class<? extends ByteBuffer>>asSet(ByteBuffer.class);
      }
   }


}
