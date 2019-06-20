package org.infinispan.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.remoting.transport.Address;

/**
 * ConsistentHashFactory implementation that allows the user to control who the owners are.
 *
 * @author Dan Berindei
 * @since 7.0
 */
@SerializeWith(ReplicatedControlledConsistentHashFactory.Externalizer.class)
public class ReplicatedControlledConsistentHashFactory implements ConsistentHashFactory<ReplicatedConsistentHash> {
   private int[] primaryOwnerIndices;

   /**
    * Create a consistent hash factory with a single segment.
    */
   public ReplicatedControlledConsistentHashFactory(int primaryOwner1, int... otherPrimaryOwners) {
      this(concatOwners(primaryOwner1, otherPrimaryOwners));
   }

   private ReplicatedControlledConsistentHashFactory(int[] primaryOwnerIndices) {
      this.primaryOwnerIndices = primaryOwnerIndices;
   }

   @Override
   public ReplicatedConsistentHash create(Hash hashFunction, int numOwners, int numSegments,
         List<Address> members, Map<Address, Float> capacityFactors) {
      int[] thePrimaryOwners = new int[primaryOwnerIndices.length];
      for (int i = 0; i < primaryOwnerIndices.length; i++) {
         thePrimaryOwners[i] = Math.min(primaryOwnerIndices[i], members.size() - 1);
      }
      return new ReplicatedConsistentHash(hashFunction, members, thePrimaryOwners);
   }

   @Override
   public ReplicatedConsistentHash updateMembers(ReplicatedConsistentHash baseCH, List<Address> newMembers,
         Map<Address, Float> capacityFactors) {
      return create(baseCH.getHashFunction(), baseCH.getNumOwners(), baseCH.getNumSegments(), newMembers,
            null);
   }

   @Override
   public ReplicatedConsistentHash rebalance(ReplicatedConsistentHash baseCH) {
      return create(baseCH.getHashFunction(), baseCH.getNumOwners(), baseCH.getNumSegments(),
            baseCH.getMembers(), null);
   }

   @Override
   public ReplicatedConsistentHash union(ReplicatedConsistentHash ch1, ReplicatedConsistentHash ch2) {
      return ch1.union(ch2);
   }

   private static int[] concatOwners(int head, int[] tail) {
      int[] firstSegmentOwners;
      if (tail == null || tail.length == 0) {
         firstSegmentOwners = new int[]{head};
      } else {
         firstSegmentOwners = new int[tail.length + 1];
         firstSegmentOwners[0] = head;
         for (int i = 0; i < tail.length; i++) {
            firstSegmentOwners[i + 1] = tail[i];
         }
      }
      return firstSegmentOwners;
   }

   public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<ReplicatedControlledConsistentHashFactory> {
      @Override
      public void writeObject(ObjectOutput output, ReplicatedControlledConsistentHashFactory object) throws IOException {
         int numberOfIndices = object.primaryOwnerIndices.length;
         MarshallUtil.marshallSize(output, numberOfIndices);
         for (int i = 0; i < numberOfIndices; i++)
            output.writeInt(object.primaryOwnerIndices[i]);
      }

      @Override
      public ReplicatedControlledConsistentHashFactory readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int numberOfIndices = MarshallUtil.unmarshallSize(input);
         int[] indices = new int[numberOfIndices];
         for (int i = 0; i < numberOfIndices; i++)
            indices[i] = input.readInt();
         return new ReplicatedControlledConsistentHashFactory(indices);
      }
   }
}
