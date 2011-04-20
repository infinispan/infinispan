/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distribution.ch;

import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.hash.Hash;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import static java.lang.Math.min;

/**
 * Consistent hash that is aware of cluster topology. Design described here: http://community.jboss.org/wiki/DesigningServerHinting.
 * <p/>
 * <pre>
 * Algorithm:
 * - place nodes on the hash wheel based address's hash code
 * - For selecting owner nodes:
 *       - pick the first one based on key's hash code
 *       - for subsequent nodes, walk clockwise and pick nodes that have a different site id
 *       - if not enough nodes found repeat walk again and pick nodes that have different site id and rack id
 *       - if not enough nodes found repeat walk again and pick nodes that have different site id, rack id and machine
 * id
 *       - Ultimately cycle back to the first node selected, don't discard any nodes, regardless of machine id/rack
 * id/site id match.
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class TopologyAwareConsistentHash extends AbstractWheelConsistentHash {
   private TopologyInfo topologyInfo;

   public TopologyAwareConsistentHash() {
   }

   public TopologyAwareConsistentHash(Hash hash) {
      setHashFunction(hash);
   }

   @Override
   public List<Address> locate(Object key, int replCount) {
      Address owner = getOwner(key);
      if (trace) log.tracef("Owner of key %s identified as %s", key, owner);
      int ownerCount = min(replCount, caches.size());
      List<Address> owners = getOwners(owner, ownerCount);
      return owners;
   }

   @Override
   public List<Address> getStateProvidersOnLeave(Address leaver, int replCount) {
      Set<Address> result = new HashSet<Address>();

      Address realLeaver = getRealAddress(leaver);
      
      //1. first get all the node that replicated on leaver
      for (Address address : caches) {
         if (address.equals(leaver)) continue;
         if (getOwners(address, replCount).contains(realLeaver)) {
            result.add(address);
         }
      }

      //2. then get first leaver's backup
      List<Address> addressList = getOwners(realLeaver, replCount);
      if (addressList.size() > 1) {
         result.add(addressList.get(1));
      }
      return new ArrayList<Address>(result);
   }


   /**
    * In this situation are the same nodes providing state on join as the nodes that provide state on leave.
    */
   @Override
   public List<Address> getStateProvidersOnJoin(Address joiner, int replCount) {
      return getStateProvidersOnLeave(joiner, replCount);
   }

   private List<Address> getOwners(Address address, int numOwners) {
      Address realAddress = getRealAddress(address);
      int ownerHash = getNormalizedHash(address);
      Collection<Address> beforeOnWheel = positions.headMap(ownerHash).values();
      Collection<Address> afterOnWheel = positions.tailMap(ownerHash).values();
      ArrayList<Address> processSequence = new ArrayList<Address>(afterOnWheel);
      processSequence.addAll(beforeOnWheel);
      List<Address> result = new ArrayList<Address>();
      result.add(getRealAddress(processSequence.remove(0)));
      int level = 0;
      while (result.size() < numOwners) {
         Iterator<Address> addrIt = processSequence.iterator();
         while (addrIt.hasNext()) {
            Address a = addrIt.next();
            Address ra = getRealAddress(a);
            switch (level) {
               case 0: { //site level
                  if (!topologyInfo.isSameSite(realAddress, ra)) {
                     if (trace) log.tracef("Owner (different site) identified as %s", a);
                     result.add(ra);
                     addrIt.remove();
                  }
                  break;
               }
               case 1: { //rack level
                  if (!topologyInfo.isSameRack(realAddress, ra)) {
                     if (trace) log.tracef("Owner (different rack) identified as %s", a);
                     result.add(ra);
                     addrIt.remove();
                  }
                  break;
               }
               case 2: { //machine level
                  if (!topologyInfo.isSameMachine(realAddress, ra)) {
                     if (trace) log.tracef("Owner (different machine) identified as %s", a);
                     result.add(ra);
                     addrIt.remove();
                  }
                  break;
               }
               case 3: { //just add them in sequence
                  if (trace) log.tracef("Owner (same machine) identified as %s", a);
                  result.add(ra);
                  addrIt.remove();
                  break;
               }
            }
            if (result.size() == numOwners) break;
         }
         level++;
      }
      //assertion
      if (result.size() != numOwners) throw new AssertionError("This should not happen!");
      return result;
   }

   private Address getOwner(Object key) {
      int hash = getNormalizedHash(key);
      SortedMap<Integer, Address> map = positions.tailMap(hash);
      if (map.size() == 0) {
         return positions.get(positions.firstKey());
      }
      Integer ownerHash = map.firstKey();
      return positions.get(ownerHash);
   }

   public static class Externalizer extends AbstractWheelConsistentHash.Externalizer<TopologyAwareConsistentHash> {
      @Override
      protected TopologyAwareConsistentHash instance() {
         return new TopologyAwareConsistentHash();
      }

      @Override
      public void writeObject(ObjectOutput output, TopologyAwareConsistentHash topologyAwareConsistentHash) throws IOException {
         super.writeObject(output, topologyAwareConsistentHash);
         Collection<NodeTopologyInfo> infoCollection = topologyAwareConsistentHash.topologyInfo.getAllTopologyInfo();
         output.writeInt(infoCollection.size());
         for (NodeTopologyInfo nti : infoCollection) output.writeObject(nti);
      }

      @Override
      public TopologyAwareConsistentHash readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         TopologyAwareConsistentHash ch = super.readObject(unmarshaller);
         ch.topologyInfo = new TopologyInfo();
         int ntiCount = unmarshaller.readInt();
         for (int i = 0; i < ntiCount; i++) {
            NodeTopologyInfo nti = (NodeTopologyInfo) unmarshaller.readObject();
            ch.topologyInfo.addNodeTopologyInfo(nti.getAddress(), nti);
         }
         return ch;
      }

      @Override
      public Integer getId() {
         return Ids.TOPOLOGY_AWARE_CH;
      }

      @Override
      public Set<Class<? extends TopologyAwareConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends TopologyAwareConsistentHash>>asSet(TopologyAwareConsistentHash.class);
      }
   }

   @Override
   public void setTopologyInfo(TopologyInfo topologyInfo) {
      this.topologyInfo = topologyInfo;
   }

   public TopologyInfo getTopologyInfo() {
      return topologyInfo;
   }

   @Override
   public String toString() {
      return "TopologyAwareConsistentHash {" +
            "addresses=" + caches +
            ", positions=" + positions +
            ", topologyInfo=" + topologyInfo +
            ", addressToHashIds=" + addressToHashIds +
            "}";
   }
   
   @Override
   protected boolean isVirtualNodesEnabled() {
      return numVirtualNodes > 1;
   }
   
}
