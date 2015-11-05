package org.infinispan.topology;

import java.util.UUID;

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
      return new PersistentUUID(UUID.randomUUID());
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
}