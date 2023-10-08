package org.infinispan.distribution.ch.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.PersistentUUID;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class AbstractConsistentHash implements ConsistentHash {
   // State constants
   protected static final String STATE_CAPACITY_FACTOR = "capacityFactor.%d";
   protected static final String STATE_CAPACITY_FACTORS = "capacityFactors";
   protected static final String STATE_NUM_SEGMENTS = "numSegments";

   /**
    * The membership of the cache topology that uses this CH.
    */
   protected final List<Address> members;
   protected final List<Float> capacityFactors;

   protected AbstractConsistentHash(int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
      if (numSegments < 1)
         throw new IllegalArgumentException("The number of segments must be strictly positive");

      this.members = new ArrayList<>(members);
      if (capacityFactors == null) {
         this.capacityFactors = null;
      } else {
         this.capacityFactors = new ArrayList<>(members.size());
         for (Address member : members)
            this.capacityFactors.add(capacityFactors.get(member));
      }
   }

   protected AbstractConsistentHash(int numSegments, List<Address> members, List<Float> capacityFactors) {
      if (numSegments < 1)
         throw new IllegalArgumentException("The number of segments must be strictly positive");

      this.members = members;
      this.capacityFactors = capacityFactors;
   }

   protected AbstractConsistentHash(ScopedPersistentState state) {
      this(parseNumSegments(state), parseMembers(state), parseCapacityFactors(state));
   }

   protected static int parseNumSegments(ScopedPersistentState state) {
      return state.getIntProperty(STATE_NUM_SEGMENTS);
   }

   protected static List<Address> parseMembers(ScopedPersistentState state) {
      int numMembers = Integer.parseInt(state.getProperty(ConsistentHashPersistenceConstants.STATE_MEMBERS));
      List<Address> members = new ArrayList<>(numMembers);
      for(int i = 0; i < numMembers; i++) {
         PersistentUUID uuid = PersistentUUID.fromString(state.getProperty(String.format(ConsistentHashPersistenceConstants.STATE_MEMBER, i)));
         members.add(uuid);
      }
      return members;
   }

   protected static List<Float> parseCapacityFactors(ScopedPersistentState state) {
      int numCapacityFactors = Integer.parseInt(state.getProperty(STATE_CAPACITY_FACTORS));
      List<Float> capacityFactors = new ArrayList<>(numCapacityFactors);
      for (int i = 0; i < numCapacityFactors; i++) {
         capacityFactors.add(Float.parseFloat(state.getProperty(String.format(STATE_CAPACITY_FACTOR, i))));
      }
      return capacityFactors;
   }

   @Override
   public void toScopedState(ScopedPersistentState state) {
      state.setProperty(ConsistentHashPersistenceConstants.STATE_CONSISTENT_HASH, this.getClass().getName());
      state.setProperty(STATE_NUM_SEGMENTS, getNumSegments());
      state.setProperty(ConsistentHashPersistenceConstants.STATE_MEMBERS, members.size());
      for (int i = 0; i < members.size(); i++) {
         state.setProperty(String.format(ConsistentHashPersistenceConstants.STATE_MEMBER, i),
            members.get(i).toString());
      }
      state.setProperty(STATE_CAPACITY_FACTORS, capacityFactors.size());
      for (int i = 0; i < capacityFactors.size(); i++) {
         state.setProperty(String.format(STATE_CAPACITY_FACTOR, i), capacityFactors.get(i));
      }
   }

   @Override
   public List<Address> getMembers() {
      return members;
   }

   /**
    * Adds all elements from <code>src</code> list that do not already exist in <code>dest</code> list to the latter.
    *
    * @param dest List where elements are added
    * @param src List of elements to add - this is never modified
    */
   protected static void mergeLists(List<Address> dest, List<Address> src) {
      for (Address node : src) {
         if (!dest.contains(node)) {
            dest.add(node);
         }
      }
   }

   public static HashMap<Address, Integer> getMemberIndexMap(List<Address> members) {
      HashMap<Address, Integer> memberIndexes = new HashMap<>(members.size());
      for (int i = 0; i < members.size(); i++) {
         memberIndexes.put(members.get(i), i);
      }
      return memberIndexes;
   }

   public Map<Address, Float> getCapacityFactors() {
      if (capacityFactors == null)
         return null;

      Map<Address, Float> capacityFactorsMap = new HashMap<>(members.size());
      for (int i = 0; i < members.size(); i++) {
         capacityFactorsMap.put(members.get(i), capacityFactors.get(i));
      }
      return capacityFactorsMap;
   }

   protected Map<Address, Float> unionCapacityFactors(AbstractConsistentHash ch2) {
      Map<Address, Float> unionCapacityFactors = null;
      if (this.capacityFactors != null || ch2.capacityFactors != null) {
         unionCapacityFactors = new HashMap<>();
         if (this.capacityFactors != null) {
            unionCapacityFactors.putAll(this.getCapacityFactors());
         } else {
            for (Address node : this.members) {
               unionCapacityFactors.put(node, 1.0f);
            }
         }
         if (ch2.capacityFactors != null) {
            unionCapacityFactors.putAll(ch2.getCapacityFactors());
         } else {
            for (Address node : ch2.members) {
               unionCapacityFactors.put(node, 1.0f);
            }
         }
      }
      return unionCapacityFactors;
   }

   protected void checkSameHashAndSegments(AbstractConsistentHash dch2) {
      int numSegments = getNumSegments();
      if (numSegments != dch2.getNumSegments()) {
         throw new IllegalArgumentException("The consistent hash objects must have the same number of segments");
      }
   }

   protected Map<Address, Float> remapCapacityFactors(UnaryOperator<Address> remapper, boolean allowMissing) {
      Map<Address, Float> remappedCapacityFactors = null;
      if (capacityFactors != null) {
         remappedCapacityFactors = new HashMap<>(members.size());
         for(int i=0; i < members.size(); i++) {
            Address a = remapper.apply(members.get(i));
            if (a == null) {
               if (allowMissing) continue;
               return null;
            }
            remappedCapacityFactors.put(a, capacityFactors.get(i));
         }
      }
      return remappedCapacityFactors;
   }

   protected List<Address> remapMembers(UnaryOperator<Address> remapper, boolean allowMissing) {
      List<Address> remappedMembers = new ArrayList<>(members.size());
      for(Iterator<Address> i = members.iterator(); i.hasNext(); ) {
         Address a = remapper.apply(i.next());
         if (a == null) {
            if (allowMissing) continue;
            return null;
         }
         remappedMembers.add(a);
      }
      return remappedMembers;
   }
}
