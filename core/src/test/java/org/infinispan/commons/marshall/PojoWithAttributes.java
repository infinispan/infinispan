package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.test.data.Key;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;

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
   final CacheEntry entry;
   final Key key;
   final UUID uuid;

   public PojoWithAttributes(int age, String key) {
      this.age = age;
      this.entry = TestInternalCacheEntryFactory.create(
            "internalkey-" + key, "internalvalue-" + age, (age * 17));
      this.key = new Key(key, false);
      this.uuid = UUID.randomUUID();
   }

   PojoWithAttributes(int age, CacheEntry entry, Key key, UUID uuid) {
      this.age = age;
      this.entry = entry;
      this.key = key;
      this.uuid = uuid;
   }

   static void writeObject(ObjectOutput output, PojoWithAttributes pojo) throws IOException {
      output.writeInt(pojo.age);
      output.writeObject(pojo.entry);
      output.writeObject(pojo.key);
      output.writeObject(pojo.uuid);
   }

   static PojoWithAttributes readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int age = input.readInt();
      CacheEntry entry = (CacheEntry) input.readObject();
      Key key = (Key) input.readObject();
      UUID uuid = (UUID) input.readObject();
      return new PojoWithAttributes(age, entry, key, uuid);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PojoWithAttributes that = (PojoWithAttributes) o;

      if (age != that.age) return false;
      if (entry != null ? !entry.equals(that.entry) : that.entry != null)
         return false;
      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = age;
      result = 31 * result + (entry != null ? entry.hashCode() : 0);
      result = 31 * result + (key != null ? key.hashCode() : 0);
      result = 31 * result + (uuid != null ? uuid.hashCode() : 0);
      return result;
   }
}
