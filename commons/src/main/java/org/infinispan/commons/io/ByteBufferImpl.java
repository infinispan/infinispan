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
import java.util.Set;

/**
 * A byte buffer that exposes the internal byte array with minimal copying
 *
 * @author (various)
 * @since 4.0
 */
public class ByteBufferImpl implements ByteBuffer {
   private final byte[] buf;
   private final int offset;
   private final int length;

   public ByteBufferImpl(byte[] buf, int offset, int length) {
      if (buf == null)
         throw new IllegalArgumentException("Null buff not allowed!");
      if (buf.length < offset + length)
         throw new IllegalArgumentException("Incorrect arguments: buff.length"
                                                  + buf.length + ", offset=" + offset +", length="+ length);
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
      byte[] new_buf = buf != null ? new byte[length] : null;
      int new_length = new_buf != null ? new_buf.length : 0;
      if (new_buf != null)
         System.arraycopy(buf, offset, new_buf, 0, length);
      return new ByteBufferImpl(new_buf, 0, new_length);
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
      if (buf == null)
         return 0;

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

   public static class Externalizer extends AbstractExternalizer<ByteBufferImpl> {

      private static final long serialVersionUID = -5291318076267612501L;

      @Override
      public void writeObject(ObjectOutput output, ByteBufferImpl b) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, b.length);
         output.write(b.buf, b.offset, b.length);
      }

      @Override
      public ByteBufferImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int length = UnsignedNumeric.readUnsignedInt(input);
         byte[] data = new byte[length];
         input.readFully(data, 0, length);
         return new ByteBufferImpl(data, 0, length);
      }

      @Override
      public Integer getId() {
         return Ids.BYTE_BUFFER;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends ByteBufferImpl>> getTypeClasses() {
         return Util.<Class<? extends ByteBufferImpl>>asSet(ByteBufferImpl.class);
      }
   }


}
