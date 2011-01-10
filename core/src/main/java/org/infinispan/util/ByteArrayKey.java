package org.infinispan.util;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Set;

/**
 * Wrapper class for byte[] keys.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class ByteArrayKey {

   private final byte[] data;

   public ByteArrayKey(byte[] data) {
      this.data = data;
   }

   public byte[] getData() {
      return data;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      ByteArrayKey key = (ByteArrayKey) obj;
      return Arrays.equals(key.data, this.data);
   }

   @Override
   public int hashCode() {
      return 41 + Arrays.hashCode(data);
   }

   @Override
   public String toString() {
      return new StringBuilder().append("ByteArrayKey").append("{")
         .append("data=").append(Util.printArray(data, true))
         .append("}").toString();
   }

   public static class Externalizer extends AbstractExternalizer<ByteArrayKey> {
      @Override
      public void writeObject(ObjectOutput output, ByteArrayKey key) throws IOException {
         output.writeInt(key.data.length);
         output.write(key.data);
      }

      @Override
      public ByteArrayKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] data = new byte[input.readInt()];
         input.readFully(data);
         return new ByteArrayKey(data);
      }

      @Override
      public Integer getId() {
         return Ids.BYTE_ARRAY_KEY;
      }

      @Override
      public Set<Class<? extends ByteArrayKey>> getTypeClasses() {
         return Util.<Class<? extends ByteArrayKey>>asSet(ByteArrayKey.class);
      }
   }

}