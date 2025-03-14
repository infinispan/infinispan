package org.infinispan.xsite.irac;

import static org.infinispan.commons.io.UnsignedNumeric.readUnsignedInt;
import static org.infinispan.commons.io.UnsignedNumeric.writeUnsignedInt;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/**
 * @author Pedro Ruivo
 * @since 14
 */
public record IracManagerKeyInfo(int segment, Object key, Object owner) {

   public IracManagerKeyInfo {
      Objects.requireNonNull(key);
      Objects.requireNonNull(owner);
   }

   public static void writeTo(ObjectOutput output, IracManagerKeyInfo keyInfo) throws IOException {
      if (keyInfo == null) {
         output.writeObject(null);
         return;
      }
      output.writeObject(keyInfo.key());
      writeUnsignedInt(output, keyInfo.segment());
      output.writeObject(keyInfo.owner());
   }

   public static IracManagerKeyInfo readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      if (key == null) {
         return null;
      }
      return new IracManagerKeyInfo(readUnsignedInt(input), key, input.readObject());
   }
}
