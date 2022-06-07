package org.infinispan.distribution.ch.impl;

import static org.infinispan.distribution.ch.impl.ConsistentHashPersistenceConstants.STATE_CONSISTENT_HASH;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import net.jcip.annotations.Immutable;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.commons.util.Immutables;
import org.infinispan.distribution.Member;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.PersistentUUID;

/**
 * An immutable {@link ConsistentHash}.
 * <p/>
 * This hash keep the association between the node's {@link Address} and {@link PersistentUUID}, which we use
 * when creating the hash to distribute the segments between the nodes.
 * <p/>
 * This hash will delegate its operations to {@link DefaultConsistentHash}, where we implement only some methods
 * to be sure we propagate the members.
 *
 * @author José Bolina
 * @since 14.0
 * @see DefaultConsistentHash
 */
@Immutable
public class SyncConsistentHash implements ConsistentHash {
   private static final String STATE_ASSOCIATION_ADDRESS_INDEX = "association.%d.uuid";

   private final List<Member> members;
   private final DefaultConsistentHash delegate;

   public SyncConsistentHash(int numOwners, int numSegments, List<Member> members, List<Member>[] owners) {
      this(members, numOwners, numSegments, collectOwners(owners));
   }

   public SyncConsistentHash(List<Member> members, int numOwners, int numSegments, List<Address>[] owners) {
      this(members, new DefaultConsistentHash(numOwners, numSegments, collectAddresses(members),
            collectCapacityFactors(members), owners));
   }

   SyncConsistentHash(ScopedPersistentState state) {
      DefaultConsistentHash delegate = new DefaultConsistentHash(state);
      List<Member> members = readState(state, delegate);
      this.delegate = delegate;
      this.members = Immutables.immutableListCopy(members);
   }

   private SyncConsistentHash(Collection<Member> members, DefaultConsistentHash delegate) {
      this.members = Immutables.immutableListConvert(members);
      this.delegate = delegate;
   }

   @Override
   public int getNumSegments() {
      return delegate.getNumSegments();
   }

   @Override
   public int getNumOwners() {
      return delegate.getNumOwners();
   }

   @Override
   public List<Address> getMembers() {
      return delegate.getMembers();
   }

   public List<Member> completeMembers() {
      return members;
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      return delegate.locateOwnersForSegment(segmentId);
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return delegate.locatePrimaryOwnerForSegment(segmentId);
   }

   @Override
   public Set<Integer> getSegmentsForOwner(Address owner) {
      return delegate.getSegmentsForOwner(owner);
   }

   @Override
   public Set<Integer> getPrimarySegmentsForOwner(Address owner) {
      return delegate.getPrimarySegmentsForOwner(owner);
   }

   @Override
   public String getRoutingTableAsString() {
      return delegate.getRoutingTableAsString();
   }

   @Override
   public void toScopedState(ScopedPersistentState state) {
      delegate.toScopedState(state);
      state.setProperty(STATE_CONSISTENT_HASH, this.getClass().getName());
      Map<Address, PersistentUUID> association = collectPersistentUuid(members);
      for (int i = 0; i < delegate.members.size(); i++) {
         PersistentUUID uuid = association.get(delegate.members.get(i));
         String stringified = uuid != null ? uuid.toString() : "";
         state.setProperty(String.format(STATE_ASSOCIATION_ADDRESS_INDEX, i), stringified);
      }
   }

   @Override
   public ConsistentHash remapAddresses(UnaryOperator<Address> remapper) {
      DefaultConsistentHash remappedDelegate = (DefaultConsistentHash) delegate.remapAddresses(remapper);

      if (remappedDelegate == null) return null;

      List<Member> remappedMembers = new ArrayList<>(members.size());
      Map<Address, PersistentUUID> association = collectPersistentUuid(members);
      for (Member member: members) {
         PersistentUUID previousUuid = association.get(member.address());
         Address remappedAddress = remapper.apply(member.address());
         Member remappedMember = new Member(remappedAddress, previousUuid, member.capacityFactor());
         remappedMembers.add(remappedMember);
      }

      return new SyncConsistentHash(remappedMembers, remappedDelegate);
   }

   @Override
   public Map<Address, Float> getCapacityFactors() {
      return delegate.getCapacityFactors();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SyncConsistentHash that = (SyncConsistentHash) o;
      return delegate.equals(that.delegate)
            && members.equals(that.members);
   }

   @Override
   public int hashCode() {
      return delegate.hashCode();
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("SyncConsistentHash{");
      sb.append("associations=[");
      boolean first = true;
      Map<Address, PersistentUUID> association = collectPersistentUuid(members);
      for (Map.Entry<Address, PersistentUUID> entry: association.entrySet()) {
         if (first) {
            first = false;
         } else {
            sb.append(", ");
         }

         sb.append("(")
               .append(entry.getKey())
               .append(", ")
               .append(entry.getValue())
               .append(")");
      }

      sb.append("]")
            .append(", delegate=")
            .append(delegate.toString())
            .append("}");
      return sb.toString();
   }

   public SyncConsistentHash union(SyncConsistentHash other) {
      DefaultConsistentHash delegateUnion = delegate.union(other.delegate);

      Set<Member> unionMembers = new HashSet<>();
      unionMembers.addAll(members);
      unionMembers.addAll(other.members);

      return new SyncConsistentHash(unionMembers, delegateUnion);
   }

   private static List<Address> collectAddresses(List<Member> members) {
      return members.stream().map(Member::address).collect(Collectors.toList());
   }

   private static Map<Address, Float> collectCapacityFactors(List<Member> members) {
      return members.stream().collect(Collectors.toMap(Member::address, Member::capacityFactor));
   }

   private static Map<Address, PersistentUUID> collectPersistentUuid(List<Member> members) {
      return members.stream()
            .filter(m -> m.uuid() != null)
            .collect(Collectors.toMap(Member::address, Member::uuid));
   }

   private static List<Member> readState(ScopedPersistentState state, DefaultConsistentHash delegate) {
      List<Address> addresses = delegate.getMembers();
      Map<Address, Float> capacityFactors = delegate.getCapacityFactors();
      List<Member> members = new ArrayList<>(addresses.size());

      for (int i = 0; i < addresses.size(); i++) {
         String stringified = state.getProperty(String.format(STATE_ASSOCIATION_ADDRESS_INDEX, i));
         Address address = addresses.get(i);
         float capacity = capacityFactors.getOrDefault(address, 1.0f);
         PersistentUUID uuid = stringified.isEmpty() ? null : PersistentUUID.fromString(stringified);
         Member member = new Member(address, uuid, capacity);
         members.add(member);
      }

      return members;
   }

   public static List<Address>[] collectOwners(List<Member>[] memberOwners) {
      List<Address>[] segmentOwners = new List[memberOwners.length];
      for (int i = 0; i < memberOwners.length; i++) {
         Address[] owners = new Address[memberOwners[i].size()];
         for (int j = 0; j < memberOwners[i].size(); j++) {
            Member member = memberOwners[i].get(j);
            owners[j] = member.address();
         }
         segmentOwners[i] = Immutables.immutableListWrap(owners);
      }
      return segmentOwners;
   }

   public static class Externalizer extends InstanceReusingAdvancedExternalizer<SyncConsistentHash> {

      @Override
      public Integer getId() {
         return Ids.SYNC_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends SyncConsistentHash>> getTypeClasses() {
         return Collections.singleton(SyncConsistentHash.class);
      }

      @Override
      public void doWriteObject(ObjectOutput output, SyncConsistentHash sync) throws IOException {
         output.writeInt(sync.getNumSegments());
         output.writeInt(sync.delegate.getNumOwners());
         output.writeObject(sync.members);

         Map<Address, Member> grouped = sync.members.stream().collect(Collectors.toMap(Member::address, Function.identity()));
         Map<Member, Integer> indexed = getMemberIndexMap(sync.members);
         for (int i = 0; i < sync.getNumSegments(); i++) {
            List<Address> owners = sync.locateOwnersForSegment(i);
            output.writeInt(owners.size());
            for (Address owner: owners) {
               output.writeInt(indexed.get(grouped.get(owner)));
            }
         }
      }

      @Override
      @SuppressWarnings("unchecked")
      public SyncConsistentHash doReadObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int numSegments = input.readInt();
         int numOwners = input.readInt();
         List<Member> members = (List<Member>) input.readObject();

         List<Member>[] segmentOwners = new List[numSegments];
         for (int i = 0; i < numSegments; i++) {
            int size = input.readInt();
            Member[] owners = new Member[size];
            for (int j = 0; j < size; j++) {
               int index = input.readInt();
               owners[j] = members.get(index);
            }
            segmentOwners[i] = Immutables.immutableListWrap(owners);
         }

         return new SyncConsistentHash(numOwners, numSegments, members, segmentOwners);
      }

      private HashMap<Member, Integer> getMemberIndexMap(List<Member> members) {
         HashMap<Member, Integer> memberIndexes = new HashMap<>(members.size());
         for (int i = 0; i < members.size(); i++) {
            memberIndexes.put(members.get(i), i);
         }
         return memberIndexes;
      }
   }
}
