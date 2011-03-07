package org.infinispan.distribution.ch;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.infinispan.util.hash.Hash;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import static java.lang.Math.min;

/**
 * Consistent hash that is aware of cluster topology.
 * Design described here: http://community.jboss.org/wiki/DesigningServerHinting.
 * <p>
 * <pre>
 * Algorithm:
 * - place nodes on the hash wheel based address's hash code
 * - For selecting owner nodes:
 *       - pick the first one based on key's hash code
 *       - for subsequent nodes, walk clockwise and pick nodes that have a different site id
 *       - if not enough nodes found repeat walk again and pick nodes that have different site id and rack id
 *       - if not enough nodes found repeat walk again and pick nodes that have different site id, rack id and machine id
 *       - Ultimately cycle back to the first node selected, don't discard any nodes, regardless of machine id/rack
 * id/site id match.
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Marshallable(externalizer = TopologyAwareConsistentHash.Externalizer.class, id = Ids.TOPOLOGY_AWARE_CH)
public class TopologyAwareConsistentHash extends AbstractWheelConsistentHash {
   public TopologyAwareConsistentHash() {
   }

   public TopologyAwareConsistentHash(Hash hash) {
      setHashFunction(hash);
   }

   public List<Address> locate(Object key, int replCount) {
      Address owner = getOwner(key);
      int ownerCount = min(replCount, addresses.size());
      return getOwners(owner, ownerCount);
   }

   public List<Address> getStateProvidersOnLeave(Address leaver, int replCount) {
      Set<Address> result = new HashSet<Address>();

      //1. first get all the node that replicated on leaver
      for (Address address : addresses) {
         if (address.equals(leaver)) continue;
         if (getOwners(address, replCount).contains(leaver)) {
            result.add(address);
         }
      }

      //2. then get first leaver's backup
      List<Address> addressList = getOwners(leaver, replCount);
      if (addressList.size() > 1) {
         result.add(addressList.get(1));
      }
      return new ArrayList<Address>(result);
   }


   /**
    * In this situation are the same nodes providing state on join as the nodes that provide state on leave.
    */
   public List<Address> getStateProvidersOnJoin(Address joiner, int replCount) {
      return getStateProvidersOnLeave(joiner, replCount);
   }

   protected List<Address> getOwners(Address address, int numOwners) {
      int ownerHash = getNormalizedHash(address);
      Collection<Address> beforeOnWheel = positions.headMap(ownerHash).values();
      Collection<Address> afterOnWheel = positions.tailMap(ownerHash).values();
      ArrayList<Address> processSequence = new ArrayList<Address>(afterOnWheel);
      processSequence.addAll(beforeOnWheel);
      List<Address> result = new ArrayList<Address>();
      result.add(processSequence.remove(0));
      int level = 0;
      while (result.size() < numOwners) {
         Iterator<Address> addrIt = processSequence.iterator();
         while (addrIt.hasNext()) {
            Address a = addrIt.next();
            switch (level) {
               case 0 :  //site level
                  if (!isSameSite(address, a)) {
                     result.add(a);
                     addrIt.remove();
                  }
                  break;
               case 1 :  //rack level
                  if (!isSameRack(address, a)) {
                     result.add(a);
                     addrIt.remove();
                  }
                  break;
               case 2 :  //machine level
                  if (!isSameMachine(address, a)) {
                     result.add(a);
                     addrIt.remove();
                  }
                  break;
               case 3 :  //just add them in sequence
                  result.add(a);
                  addrIt.remove();
                  break;
            }
            if (result.size() == numOwners) break;
         }
         level++;
      }
      //assertion
      if (result.size() != numOwners) throw new AssertionError("This should not happen!");
      return result;
   }

   protected Address getOwner(Object key) {
      int hash = getNormalizedHash(key);
      SortedMap<Integer, Address> map = positions.tailMap(hash);
      if (map.isEmpty()) {
         return positions.get(positions.firstKey());
      }
      Integer ownerHash = map.firstKey();
      return positions.get(ownerHash);
   }


   protected static boolean isSameSite(Address one, Address two) {
      return one != null && two != null && ((TopologyAwareAddress)one).isSameSite((TopologyAwareAddress) two);
   }

   protected static boolean isSameRack(Address one, Address two) {
      return one != null && two != null && ((TopologyAwareAddress)one).isSameRack((TopologyAwareAddress) two);
   }

   protected static boolean isSameMachine(Address one, Address two) {
      return one != null && two != null && ((TopologyAwareAddress)one).isSameMachine((TopologyAwareAddress) two);
   }


   public static class Externalizer extends AbstractWheelConsistentHash.Externalizer {

      @Override
      protected AbstractWheelConsistentHash instance() {
         return new TopologyAwareConsistentHash();
      }

      @Override
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         super.writeObject(output, subject);
         TopologyAwareConsistentHash dch = (TopologyAwareConsistentHash) subject;
         Collection<NodeTopologyInfo> infoCollection = dch.topologyInfo.getAllTopologyInfo();
         output.writeInt(infoCollection.size());
         for (NodeTopologyInfo nti : infoCollection) output.writeObject(nti);
      }

      @Override
      public Object readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         TopologyAwareConsistentHash ch = (TopologyAwareConsistentHash) super.readObject(unmarshaller);
         ch.topologyInfo = new TopologyInfo();
         int ntiCount = unmarshaller.readInt();
         for (int i = 0; i < ntiCount; i++) {
            NodeTopologyInfo nti = (NodeTopologyInfo) unmarshaller.readObject();
            ch.topologyInfo.addNodeTopologyInfo(nti.getAddress(), nti);
         }
         return ch;
      }
   }
}
