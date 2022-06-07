package org.infinispan.distribution;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import net.jcip.annotations.Immutable;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.PersistentUUID;

/**
 * Represents a single node during the hashing steps.
 * <p/>
 * For each node, it contains the {@link Address}, the node's {@link PersistentUUID} provided by the
 * {@link org.infinispan.topology.PersistentUUIDManager}, and the node's capacity factor.
 *
 * @author José Bolina
 * @since 14.0
 */
@Immutable
public class Member {

   private final Address address;
   private final PersistentUUID uuid;
   private final float capacityFactor;

   public Member(Address address, PersistentUUID uuid, float capacityFactor) {
      this.address = address;
      this.uuid = uuid;
      this.capacityFactor = capacityFactor;
   }

   public Address address() {
      return address;
   }

   public PersistentUUID uuid() {
      return uuid;
   }

   public float capacityFactor() {
      return capacityFactor;
   }

   @Override
   public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Member that = (Member) o;

      return address().equals(that.address)
            && capacityFactor == that.capacityFactor;
   }

   @Override
   public int hashCode() {
      int result = Math.round(capacityFactor);
      result = 31 * result + address.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "Member{" +
            "address=" + address +
            ", uuid=" + uuid +
            ", capacityFactor=" + capacityFactor +
            '}';
   }

   public static final class Externalizer extends InstanceReusingAdvancedExternalizer<Member> {

      public Externalizer() {
         super(false);
      }

      @Override
      public Integer getId() {
         return Ids.HASHING_MEMBER;
      }

      @Override
      public Set<Class<? extends Member>> getTypeClasses() {
         return Collections.singleton(Member.class);
      }

      @Override
      public void doWriteObject(ObjectOutput output, Member member) throws IOException {
         output.writeObject(member.address);
         output.writeObject(member.uuid);
         output.writeFloat(member.capacityFactor);
      }

      @Override
      public Member doReadObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Address address = (Address) input.readObject();
         PersistentUUID uuid = (PersistentUUID) input.readObject();
         float capacityFactor = input.readFloat();
         return new Member(address, uuid, capacityFactor);
      }
   }
}
