package org.infinispan.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;

/**
 * Wrapper class for byte[] keys.
 *
 * The class can be marshalled either via its externalizer or via the JVM
 * serialization.  The reason for supporting both methods is to enable
 * third-party libraries to be able to marshall/unmarshall them using standard
 * JVM serialization rules.  The Infinispan marshalling layer will always
 * chose the most performant one, aka the externalizer method.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public final class ByteArrayKey implements Serializable {

   private static final long serialVersionUID = 7305972805432411725L;
   private final byte[] data;
   private final int hashCode;

   public ByteArrayKey(byte[] data) {
      this.data = data;
      this.hashCode = 41 + Arrays.hashCode(data);
   }

   public byte[] getData() {
      return data;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || ByteArrayKey.class != obj.getClass()) return false;
      ByteArrayKey key = (ByteArrayKey) obj;
      return Arrays.equals(key.data, this.data);
   }

   @Override
   public int hashCode() {
      return hashCode;
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
         return 57;
      }

      @Override
      public Set<Class<? extends ByteArrayKey>> getTypeClasses() {
         return Util.<Class<? extends ByteArrayKey>>asSet(ByteArrayKey.class);
      }
   }
}
