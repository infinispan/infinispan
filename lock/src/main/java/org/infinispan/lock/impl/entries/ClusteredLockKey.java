package org.infinispan.lock.impl.entries;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * Used to retrieve and identify a lock in the cache
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_LOCK_KEY)
public class ClusteredLockKey {

   @ProtoField(1)
   final ByteString name;

   @ProtoFactory
   public ClusteredLockKey(ByteString name) {
      this.name = Objects.requireNonNull(name);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      ClusteredLockKey that = (ClusteredLockKey) o;

      return name.equals(that.name);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name);
   }

   @Override
   public String toString() {
      return "ClusteredLockKey{" +
            "name=" + name +
            '}';
   }

   public ByteString getName() {
      return name;
   }
}
