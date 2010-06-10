package org.infinispan.util;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Wrapper class for byte[] keys.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Marshallable(externalizer = ByteArrayKey.Externalizer.class, id = Ids.BYTE_ARRAY_KEY)
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

   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         ByteArrayKey key = (ByteArrayKey) object;
         output.writeInt(key.data.length);
         output.write(key.data);
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] data = new byte[input.readInt()];
         input.readFully(data);
         return new ByteArrayKey(data);
      }
   }
   
}
