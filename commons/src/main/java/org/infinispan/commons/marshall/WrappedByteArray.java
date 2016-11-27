package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * Simple wrapper around a byte[] to provide equals and hashCode semantics
 * @author wburns
 * @since 9.0
 */
public class WrappedByteArray implements WrappedBytes {
   private final byte[] bytes;
   private transient int hashCode;

   public WrappedByteArray(byte[] bytes) {
      this.bytes = bytes;
      this.hashCode = Arrays.hashCode(bytes);
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

   public static class Externalizer extends AbstractExternalizer<WrappedByteArray> {

      @Override
      public Set<Class<? extends WrappedByteArray>> getTypeClasses() {
         return Collections.singleton(WrappedByteArray.class);
      }

      @Override
      public Integer getId() {
         return Ids.WRAPPED_BYTE_ARRAY;
      }

      @Override
      public void writeObject(ObjectOutput output, WrappedByteArray object) throws IOException {
         MarshallUtil.marshallByteArray(object.bytes, output);
      }

      @Override
      public WrappedByteArray readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] bytes = MarshallUtil.unmarshallByteArray(input);
         return new WrappedByteArray(bytes);
      }
   }
}
