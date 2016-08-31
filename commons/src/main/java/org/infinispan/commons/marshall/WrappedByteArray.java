package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Simple wrapper around a byte[] to provide equals and hashCode semantics
 * @author wburns
 * @since 9.0
 */
public class WrappedByteArray implements WrappedBytes, Serializable {
   private final byte[] bytes;
   private transient int hashCode;

   public WrappedByteArray(byte[] bytes) {
      this.bytes = bytes;
      this.hashCode = Arrays.hashCode(bytes);
   }

   private void readObject(java.io.ObjectInputStream stream)
         throws IOException, ClassNotFoundException {
      stream.defaultReadObject();
      hashCode = Arrays.hashCode(bytes);
   }

   public byte[] getBytes() {
      return bytes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      WrappedByteArray that = (WrappedByteArray) o;

      return Arrays.equals(bytes, that.bytes);

   }

   @Override
   public int hashCode() {
      return hashCode;
   }
}
