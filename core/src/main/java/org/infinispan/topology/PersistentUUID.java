package org.infinispan.topology;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

/**
 * PersistentUUID. A special {@link Address} UUID whose purpose is to remain unchanged across node
 * restarts when using global state.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class PersistentUUID implements Address {
   final UUID uuid;
   final int hashCode;

   private PersistentUUID(UUID uuid) {
      this.uuid = uuid;
      this.hashCode = uuid.hashCode();
   }

   public PersistentUUID(long msb, long lsb) {
      this(new UUID(msb, lsb));
   }

   public static PersistentUUID randomUUID() {
      return new PersistentUUID(Util.threadLocalRandomUUID());
   }

   public static PersistentUUID fromString(String name) {
      return new PersistentUUID(UUID.fromString(name));
   }


   public long getMostSignificantBits() {
      return uuid.getMostSignificantBits();
   }

   public long getLeastSignificantBits() {
      return uuid.getLeastSignificantBits();
   }

   @Override
   public int compareTo(Address o) {
      PersistentUUID other = (PersistentUUID) o;
      return uuid.compareTo(other.uuid);
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public String toString() {
      return uuid.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      PersistentUUID other = (PersistentUUID) obj;
      if (uuid == null) {
         if (other.uuid != null)
            return false;
      } else if (!uuid.equals(other.uuid))
         return false;
      return true;
   }

   public static class Externalizer extends AbstractExternalizer<PersistentUUID> {

      @Override
      public Set<Class<? extends PersistentUUID>> getTypeClasses() {
         return Collections.<Class<? extends PersistentUUID>>singleton(PersistentUUID.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, PersistentUUID uuid) throws IOException {
         output.writeLong(uuid.getMostSignificantBits());
         output.writeLong(uuid.getLeastSignificantBits());
      }

      @Override
      public PersistentUUID readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new PersistentUUID(input.readLong(), input.readLong());
      }

      @Override
      public Integer getId() {
         return Ids.PERSISTENT_UUID;
      }
   }
}
