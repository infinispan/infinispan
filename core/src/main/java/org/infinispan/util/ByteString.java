package org.infinispan.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A simple class which encapsulates a byte[] representation of a String using a predefined encoding (currently UTF-8).
 * This avoids repeated invocation of the expensive {@link ObjectOutput#writeUTF(String)} on marshalling
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class ByteString {
   private static final Charset CHARSET = StandardCharsets.UTF_8;
   private static final ByteString EMPTY = new ByteString(new byte[0]);
   private final byte[] b;
   private String s;
   private final transient int hash;

   private ByteString(byte[] b) {
      if (b.length > 255) {
         throw new IllegalArgumentException("ByteString must be shorter than 255 bytes");
      }
      this.b = b;
      this.hash = Arrays.hashCode(b);
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
      return Arrays.equals(b, that.b);
   }

   @Override
   public String toString() {
      if (s == null) {
         s = new String(b, CHARSET);
      }
      return s;
   }


   public static void writeObject(ObjectOutput output, ByteString object) throws IOException {
      output.writeByte(object.b.length);
      if (object.b.length > 0) {
         output.write(object.b);
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
