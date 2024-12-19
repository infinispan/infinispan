package org.infinispan.topology;

import java.util.UUID;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * PersistentUUID. A special {@link Address} UUID whose purpose is to remain unchanged across node
 * restarts when using global state.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@ProtoTypeId(ProtoStreamTypeIds.PERSISTENCE_UUID)
public class PersistentUUID implements Address {
   final UUID uuid;
   final int hashCode;

   private PersistentUUID(UUID uuid) {
      this.uuid = uuid;
      this.hashCode = uuid.hashCode();
   }

   @ProtoFactory
   public PersistentUUID(long mostSignificantBits, long leastSignificantBits) {
      this(new UUID(mostSignificantBits, leastSignificantBits));
   }

   public static PersistentUUID randomUUID() {
      return new PersistentUUID(Util.threadLocalRandomUUID());
   }

   public static PersistentUUID fromString(String name) {
      return new PersistentUUID(UUID.fromString(name));
   }


   @ProtoField(number = 1, defaultValue = "-1")
   public long getMostSignificantBits() {
      return uuid.getMostSignificantBits();
   }

   @ProtoField(number = 2, defaultValue = "-2")
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
