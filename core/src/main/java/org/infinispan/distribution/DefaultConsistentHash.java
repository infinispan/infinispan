package org.infinispan.distribution;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

@Marshallable(externalizer = DefaultConsistentHash.Externalizer.class, id = Ids.DEFAULT_CONSISTENT_HASH)
public class DefaultConsistentHash extends AbstractConsistentHash {

   // make sure all threads see the current list
   ArrayList<Address> addresses;
   SortedMap<Integer, Address> positions;

   final static int HASH_SPACE = 10240; // no more than 10k nodes?


   public void setCaches(Collection<Address> caches) {

      addresses = new ArrayList<Address>(caches);

      // this list won't grow.
      addresses.trimToSize();

      positions = new TreeMap<Integer, Address>();

      for (Address a : addresses) {
         int positionIndex = Math.abs(a.hashCode()) % HASH_SPACE;
         // this is deterministic since the address list is ordered and the order is consistent across the grid
         while (positions.containsKey(positionIndex)) positionIndex = positionIndex + 1 % HASH_SPACE;
         positions.put(positionIndex, a);
      }
   }

   public Collection<Address> getCaches() {
      return addresses;
   }

   public List<Address> locate(Object key, int replCount) {
      int hash = Math.abs(key.hashCode());
      int clusterSize = addresses.size();
      int numCopiesToFind = Math.min(replCount, clusterSize);

      List<Address> owners = new ArrayList<Address>(numCopiesToFind);

      SortedMap<Integer, Address> candidates = positions.tailMap(hash % HASH_SPACE);

      for (Address a : candidates.values()) {
         if (owners.size() < numCopiesToFind)
            owners.add(a);
         else
            break;
      }

      if (owners.size() < numCopiesToFind) {
         for (Address a : positions.values()) {
            if (owners.size() < numCopiesToFind)
               owners.add(a);
            else
               break;
         }
      }

      return owners;
   }

//   public List<Address> locate(Object key, int replicationCount) {
//      int hash = Math.abs(key.hashCode());
//      int clusterSize = addresses.size();
//      int numCopiesToFind = Math.min(replicationCount, clusterSize);
//
//      List<Address> results = new ArrayList<Address>(numCopiesToFind);
//
//      int copyNumber = 0;
//
//      while (results.size() < numCopiesToFind) {
//         // we mod the index the 2nd time to make sure the index starts again from the beginning when it reaches the end.
//         // e.g., in a cluster of 10 with 3 copies of data, and a key that maps to node index 9, the next 2 backups should
//         // be at indexes 0 and 1.
//
//         int index = ((hash % clusterSize) + copyNumber) % clusterSize;
//         Address candidate = addresses.get(index);
//         results.add(candidate);
//         copyNumber++;
//      }
//
//      return results;
//   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DefaultConsistentHash that = (DefaultConsistentHash) o;

      if (addresses != null ? !addresses.equals(that.addresses) : that.addresses != null) return false;
      if (positions != null ? !positions.equals(that.positions) : that.positions != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = addresses != null ? addresses.hashCode() : 0;
      result = 31 * result + (positions != null ? positions.hashCode() : 0);
      return result;
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         DefaultConsistentHash dch = (DefaultConsistentHash) subject;
         output.writeObject(dch.addresses);
         output.writeObject(dch.positions);
      }

      @SuppressWarnings("unchecked")
      public Object readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         DefaultConsistentHash dch = new DefaultConsistentHash();
         dch.addresses = (ArrayList<Address>) unmarshaller.readObject();
         dch.positions = (SortedMap<Integer, Address>) unmarshaller.readObject();
         return dch;
      }
   }

   @Override
   public String toString() {
      return "DefaultConsistentHash{" +
            "addresses (in order of hash space position)=" + positions.values() +
            '}';
   }

   public boolean isInSameSubspace(Address a1, Address a2) {
      throw new UnsupportedOperationException("TODO Implement me!");
   }
}
