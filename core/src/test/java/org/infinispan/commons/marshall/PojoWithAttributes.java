package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import org.infinispan.commons.util.Util;
import org.infinispan.test.data.Key;

/**
 * A test pojo with references to variables that are marshalled in different
 * ways, including: primitives, objects that are marshalled with internal
 * externalizers, objects that are {@link java.io.Externalizable} and objects
 * that are {@link java.io.Serializable}
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class PojoWithAttributes {
   final int age;
   final Key key;
   final UUID uuid;

   public PojoWithAttributes(int age, String key) {
      this.age = age;
      this.key = new Key(key);
      this.uuid = Util.threadLocalRandomUUID();
   }

   PojoWithAttributes(int age, Key key, UUID uuid) {
      this.age = age;
      this.key = key;
      this.uuid = uuid;
   }

   public static void writeObject(ObjectOutput output, PojoWithAttributes pojo) throws IOException {
      output.writeInt(pojo.age);
      output.writeObject(pojo.key);
      output.writeObject(pojo.uuid);
   }

   public static PojoWithAttributes readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int age = input.readInt();
      Key key = (Key) input.readObject();
      UUID uuid = (UUID) input.readObject();
      return new PojoWithAttributes(age, key, uuid);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PojoWithAttributes that = (PojoWithAttributes) o;

      if (age != that.age) return false;
      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = age;
      result = 31 * result + (key != null ? key.hashCode() : 0);
      result = 31 * result + (uuid != null ? uuid.hashCode() : 0);
      return result;
   }
}
