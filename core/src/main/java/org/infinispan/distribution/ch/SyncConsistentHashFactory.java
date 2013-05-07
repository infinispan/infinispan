/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distribution.ch;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.infinispan.commons.hash.Hash;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;

/**
 * A {@link ConsistentHashFactory} implementation that guarantees caches with the same members
 * have the same consistent hash.
 *
 * It has a drawback compared to {@link DefaultConsistentHashFactory}, though: it can potentially
 * move a lot more segments during a rebalance than strictly necessary.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class SyncConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash> {

   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {

      Builder builder = new Builder(hashFunction, numOwners, numSegments, members);
      SortedMap<Integer, Address> primarySegments = populatePrimarySegments(builder);
      if (numSegments >= members.size()) {
         populateOwnersManySegments(builder, primarySegments);
      } else {
         populateOwnersFewSegments(builder, primarySegments);
      }

      return new DefaultConsistentHash(hashFunction, numOwners, numSegments, members, builder.getAllOwners());
   }

   protected void populateOwnersFewSegments(Builder builder, SortedMap<Integer, Address> primarySegments) {
      // Too few segments for each member to have one "primary segment",
      // but we may still have enough segments for each member to be a backup owner.

      // Populate the primary owners first - because numSegments < numMembers we're guaranteed to
      // set the primary owner of each segment
      for (Map.Entry<Integer, Address> e : primarySegments.entrySet()) {
         Integer segment = e.getKey();
         Address primaryOwner = e.getValue();
         builder.getOwners(segment).add(primaryOwner);
      }

      // Continue with the backup owners. Assign each member as owner to one segment,
      // then repeat until each segment has numOwners owners.
      boolean modified = true;
      while (modified) {
         modified = false;
         for (Address member : builder.getSortedMembers()) {
            // Compute an initial segment and iterate backwards to make it more like the other case
            int initSegment = normalizedHash(builder.getHashFunction(), member.hashCode()) / builder.getSegmentSize();
            for (int i = 0; i < builder.getNumSegments(); i++) {
               int segment = (builder.getNumSegments() + initSegment - i) % builder.getNumSegments();
               List<Address> owners = builder.getOwners(segment);
               if (owners.size() < builder.getActualNumOwners() && !owners.contains(member)) {
                  owners.add(member);
                  modified = true;
                  break;
               }
            }
         }
      }
   }

   protected int normalizedHash(Hash hashFunction, int hashcode) {
      return hashFunction.hash(hashcode) & Integer.MAX_VALUE;
   }

   protected void populateOwnersManySegments(Builder builder, SortedMap<Integer, Address> primarySegments) {
      // Each member is present at least once in the primary segments map, so we can use that
      // to populate the owner lists. For each segment assign the owners of the next numOwners
      // "primary segments" as owners.
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);
         for (Address a : primarySegments.tailMap(segment).values()) {
            if (owners.size() >= builder.getActualNumOwners())
               break;
            if (!owners.contains(a)) {
               owners.add(a);
            }
         }
         if (owners.size() < builder.getActualNumOwners()) {
            for (Address a : primarySegments.headMap(segment).values()) {
               if (owners.size() >= builder.getActualNumOwners())
                  break;
               if (!owners.contains(a)) {
                  owners.add(a);
               }
            }
         }
      }
   }

   /**
    * Finds a unique "primary segment" for each virtual member
    */
   private SortedMap<Integer, Address> populatePrimarySegments(Builder builder) {
      // Only used for debugging
      int collisions = 0;

      // Since the number of segments is potentially much larger than the number of members,
      // we need a concept of "virtual nodes" to help split the segments more evenly.
      // However, we don't have a "numVirtualNodes" setting any more, so we try to guess it
      // based on numSegments. This is not perfect because we may end up with too many virtual nodes,
      // but the only downside in that is a little more shuffling when a node joins/leaves.
      int numSegments = builder.getNumSegments();
      int numVirtualNodes = (int) (Math.log(builder.getNumOwners() * numSegments + 1) / Math.log(2)) + 1;
      int numNodes = builder.getSortedMembers().size();
      Map<Integer, Address> primarySegments = new HashMap<Integer, Address>(numNodes * numVirtualNodes);

      for (int virtualNode = 0; virtualNode < numVirtualNodes; virtualNode++) {
         for (Address member : builder.getSortedMembers()) {
            // Add the virtual node count after applying MurmurHash on the node's hashCode
            // to make up for badly spread test addresses.
            int virtualNodeHash = normalizedHash(builder.getHashFunction(), member.hashCode());
            if (virtualNode != 0) {
               virtualNodeHash = normalizedHash(builder.getHashFunction(), virtualNodeHash + virtualNode);
            }
            int initSegment = virtualNodeHash / builder.getSegmentSize();
            for (int i = 0; i < numSegments; i++) {
               int segment = (initSegment + i) % numSegments;
               if (!primarySegments.containsKey(segment)) {
                  primarySegments.put(segment, member);
                  if (segment != initSegment) collisions++;
                  break;
               }
            }
         }
      }

      return new TreeMap<Integer, Address>(primarySegments);
   }

   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> newMembers) {
      // the ConsistentHashFactory contract says we should return the same instance if we're not making changes
      if (newMembers.equals(baseCH.getMembers()))
         return baseCH;

      int numSegments = baseCH.getNumSegments();
      int numOwners = baseCH.getNumOwners();

      // we assume leavers are far fewer than members, so it makes sense to check for leavers
      HashSet<Address> leavers = new HashSet<Address>(baseCH.getMembers());
      leavers.removeAll(newMembers);

      // create a new "balanced" CH in case we need to allocate new owners for segments with 0 owners
      DefaultConsistentHash rebalancedCH = create(baseCH.getHashFunction(), numOwners, numSegments, newMembers);

      // remove leavers
      List<Address>[] newSegmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = new ArrayList<Address>(baseCH.locateOwnersForSegment(i));
         owners.removeAll(leavers);
         if (!owners.isEmpty()) {
            newSegmentOwners[i] = owners;
         } else {
            // this segment has 0 owners, fix it
            newSegmentOwners[i] = rebalancedCH.locateOwnersForSegment(i);
         }
      }

      return new DefaultConsistentHash(baseCH.getHashFunction(), numOwners, numSegments, newMembers,
            newSegmentOwners);
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      DefaultConsistentHash rebalancedCH = create(baseCH.getHashFunction(), baseCH.getNumOwners(), baseCH.getNumSegments(), baseCH.getMembers());

      // the ConsistentHashFactory contract says we should return the same instance if we're not making changes
      if (rebalancedCH.equals(baseCH))
         return baseCH;

      return rebalancedCH;
   }

   @Override
   public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
      return ch1.union(ch2);
   }

   protected static class Builder {
      private final Hash hashFunction;
      private final int numOwners;
      private final int actualNumOwners;
      private final int numSegments;
      private final List<Address> sortedMembers;
      private final int segmentSize;
      private final List<Address>[] segmentOwners;

      private Builder(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
         this.hashFunction = hashFunction;
         this.numSegments = numSegments;
         this.numOwners = numOwners;
         this.actualNumOwners = Math.min(numOwners, members.size());
         this.sortedMembers = sort(members);
         this.segmentSize = (int)Math.ceil((double)Integer.MAX_VALUE / numSegments);
         this.segmentOwners = new List[numSegments];
         for (int i = 0; i < numSegments; i++) {
            segmentOwners[i] = new ArrayList<Address>(actualNumOwners);
         }
      }

      public Hash getHashFunction() {
         return hashFunction;
      }

      public int getNumOwners() {
         return numOwners;
      }

      public int getActualNumOwners() {
         return actualNumOwners;
      }

      public int getNumSegments() {
         return numSegments;
      }

      public List<Address> getSortedMembers() {
         return sortedMembers;
      }

      public int getSegmentSize() {
         return segmentSize;
      }

      public List<Address>[] getAllOwners() {
         return segmentOwners;
      }

      public List<Address> getOwners(int i) {
         return segmentOwners[i];
      }

      private List<Address> sort(List<Address> list) {
         ArrayList<Address> result = new ArrayList<Address>(list);
         Collections.sort(result);
         return result;
      }
   }
   
   public static class Externalizer extends AbstractExternalizer<SyncConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, SyncConsistentHashFactory chf) {
      }

      @Override
      @SuppressWarnings("unchecked")
      public SyncConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new SyncConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.SYNC_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends SyncConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends SyncConsistentHashFactory>>singleton(SyncConsistentHashFactory.class);
      }
   }
}
