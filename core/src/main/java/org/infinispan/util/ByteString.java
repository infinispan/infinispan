package org.infinispan.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.infinispan.commons.util.Util;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * A simple class which encapsulates a byte[] representation of a String using a predefined encoding (currently UTF-8).
 * This avoids repeated invocation of the expensive {@link ObjectOutput#writeUTF(String)} on marshalling
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class ByteString {
   private static final Charset CHARSET = StandardCharsets.UTF_8;
   private static final ByteString EMPTY = new ByteString(Util.EMPTY_BYTE_ARRAY);
   private String s;
   private transient int hash;
   private byte[] bytes;

   ByteString() {}

   private ByteString(byte[] bytes) {
      if (bytes.length > 255) {
         throw new IllegalArgumentException("ByteString must be shorter than 255 bytes");
      }
      setBytes(bytes);
   }

   public static ByteString fromString(String s) {
      if (s.length() == 0)
         return EMPTY;
      else
         return new ByteString(s.getBytes(CHARSET));
   }

   public static ByteString emptyString() {
      return EMPTY;
   }

   @Override
   public int hashCode() {
      return hash;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ByteString that = (ByteString) o;
      return Arrays.equals(bytes, that.bytes);
   }

   @Override
   public String toString() {
      if (s == null) {
         s = new String(bytes, CHARSET);
      }
      return s;
   }

   @ProtoField(number = 1)
   byte[] getBytes() {
      return bytes;
   }

   void setBytes(byte[] bytes) {
      this.bytes = bytes;
      this.hash = Arrays.hashCode(bytes);
   }

   public static void writeObject(ObjectOutput output, ByteString object) throws IOException {
      output.writeByte(object.bytes.length);
      if (object.bytes.length > 0) {
         output.write(object.bytes);
      }
   }

   public static ByteString readObject(ObjectInput input) throws IOException {
      int len = input.readUnsignedByte();
      if (len == 0)
         return EMPTY;

      byte[] b = new byte[len];
      input.readFully(b);
      return new ByteString(b);
   }
}
