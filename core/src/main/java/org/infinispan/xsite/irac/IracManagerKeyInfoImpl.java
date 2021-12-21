package org.infinispan.xsite.irac;

import static org.infinispan.commons.io.UnsignedNumeric.readUnsignedInt;
import static org.infinispan.commons.io.UnsignedNumeric.writeUnsignedInt;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/**
 * Default implementation of {@link IracManagerKeyInfo}.
 *
 * @author Pedro Ruivo
 * @since 14
 */
public class IracManagerKeyInfoImpl implements IracManagerKeyInfo {

   private final int segment;
   private final Object key;
   private final Object owner;

   public IracManagerKeyInfoImpl(int segment, Object key, Object owner) {
      this.segment = segment;
      this.key = Objects.requireNonNull(key);
      this.owner = Objects.requireNonNull(owner);
   }

   @Override
   public Object getKey() {
      return key;
   }

   @Override
   public Object getOwner() {
      return owner;
   }

   @Override
   public int getSegment() {
      return segment;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof IracManagerKeyInfo)) return false;

      IracManagerKeyInfo that = (IracManagerKeyInfo) o;

      if (segment != that.getSegment()) return false;
      if (!key.equals(that.getKey())) return false;
      return owner.equals(that.getOwner());
   }

   @Override
   public int hashCode() {
      int result = segment;
      result = 31 * result + key.hashCode();
      result = 31 * result + owner.hashCode();
      return result;
   }

   public static void writeTo(IracManagerKeyInfo keyInfo, ObjectOutput output) throws IOException {
      if (keyInfo == null) {
         output.writeObject(null);
         return;
      }
      output.writeObject(keyInfo.getKey());
      writeUnsignedInt(output, keyInfo.getSegment());
      output.writeObject(keyInfo.getOwner());
   }

   public static IracManagerKeyInfo readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      if (key == null) {
         return null;
      }
      return new IracManagerKeyInfoImpl(readUnsignedInt(input), key, input.readObject());
   }
}
