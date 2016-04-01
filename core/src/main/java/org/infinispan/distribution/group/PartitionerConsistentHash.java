package org.infinispan.distribution.group;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.PersistentUUIDManager;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * {@link ConsistentHash} wrapper that uses a {@link KeyPartitioner} instead of a {@link Hash}.
 *
 * @author Dan Berindei
 * @since 8.2
 * @private
 */
public class PartitionerConsistentHash implements ConsistentHash {
   private final ConsistentHash ch;
   private final KeyPartitioner keyPartitioner;

   public PartitionerConsistentHash(ConsistentHash ch, KeyPartitioner keyPartitioner) {
      this.ch = Objects.requireNonNull(ch);
      this.keyPartitioner = Objects.requireNonNull(keyPartitioner);
   }

   @Override
   public int getNumSegments() {
      return ch.getNumSegments();
   }

   @Override
   public int getNumOwners() {
      return ch.getNumOwners();
   }

   @Override
   public List<Address> getMembers() {
      return ch.getMembers();
   }

   @Override
   public int getSegment(Object key) {
      return keyPartitioner.getSegment(key);
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      return ch.locateOwnersForSegment(segmentId);
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return ch.locatePrimaryOwnerForSegment(segmentId);
   }

   @Override
   public boolean isSegmentLocalToNode(Address nodeAddress, int segmentId) {
      return ch.isSegmentLocalToNode(nodeAddress, segmentId);
   }

   @Override
   public boolean isReplicated() {
      return ch.isReplicated();
   }

   @Override
   public Set<Integer> getSegmentsForOwner(Address owner) {
      return ch.getSegmentsForOwner(owner);
   }

   @Override
   public Set<Integer> getPrimarySegmentsForOwner(Address owner) {
      return ch.getPrimarySegmentsForOwner(owner);
   }

   @Override
   public String getRoutingTableAsString() {
      return ch.getRoutingTableAsString();
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      int segment = ch.isReplicated() ? 0 : getSegment(key);
      return ch.isSegmentLocalToNode(nodeAddress, segment);
   }

   @Override
   public Hash getHashFunction() {
      return ch.getHashFunction();
   }

   public KeyPartitioner getKeyPartitioner() {
      return keyPartitioner;
   }

   @Override
   public void toScopedState(ScopedPersistentState state) {
      ch.toScopedState(state);
   }

   @Override
   public ConsistentHash remapAddresses(UnaryOperator<Address> remapper) {
      return ch.remapAddresses(remapper);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      PartitionerConsistentHash that = (PartitionerConsistentHash) o;

      if (!ch.equals(that.ch))
         return false;
      return keyPartitioner.equals(that.keyPartitioner);

   }

   @Override
   public int hashCode() {
      int result = ch.hashCode();
      result = 31 * result + keyPartitioner.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "PartitionerConsistentHash:" + ch;
   }
}
