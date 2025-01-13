package org.infinispan.util;

import java.io.ObjectOutput;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A simple class which encapsulates a byte[] representation of a String using a predefined encoding (currently UTF-8).
 * This avoids repeated invocation of the expensive {@link ObjectOutput#writeUTF(String)} on marshalling
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.BYTE_STRING)
public final class ByteString implements Comparable<ByteString> {
   private static final Charset CHARSET = StandardCharsets.UTF_8;
   private static final ByteString EMPTY = new ByteString(Util.EMPTY_BYTE_ARRAY);
   private static final int MAX_LENGTH = 255;
   private transient String s;
   private transient final int hash;

   final byte[] bytes;

   ByteString(byte[] bytes) {
      if (bytes.length > MAX_LENGTH) {
         throw new IllegalArgumentException("ByteString must be less than 256 bytes");
      }
      this.bytes = bytes;
      this.hash = Arrays.hashCode(bytes);
   }

   @ProtoFactory
   static ByteString protoFactory(byte[] bytes) {
      if (bytes == null || bytes.length == 0)
         return EMPTY;
      return new ByteString(bytes);
   }

   @ProtoField(number = 1)
   byte[] getBytes() {
      return bytes.length == 0 ? null : bytes;
   }

   public static ByteString fromString(String s) {
      if (s.isEmpty())
         return EMPTY;
      else
         return new ByteString(s.getBytes(CHARSET));
   }

   public static boolean isValid(String s) {
      return s.getBytes(CHARSET).length <= MAX_LENGTH;
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

   @Override
   public int compareTo(ByteString o) {
      int ourLength = bytes.length;
      int otherLength = o.bytes.length;
      int compare;
      if ((compare = Integer.compare(ourLength, otherLength)) != 0) {
         return compare;
      }
      for (int i = 0; i < ourLength; ++i) {
         if ((compare = Byte.compare(bytes[i], o.bytes[i])) != 0) {
            return compare;
         }
      }
      return 0;
   }
}
