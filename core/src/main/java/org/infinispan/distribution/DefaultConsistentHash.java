package org.infinispan.distribution;

import org.infinispan.remoting.transport.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultConsistentHash implements ConsistentHash {
   // make sure all threads see the current list
   volatile ArrayList<Address> addresses;

   public void setCaches(Collection<Address> caches) {
      addresses = new ArrayList<Address>(caches);

      // this list won't grow.
      addresses.trimToSize();
   }

   public List<Address> locate(Object key, int replicationCount) {
      int hash = Math.abs(key.hashCode());
      int clusterSize = addresses.size();
      int numCopiesToFind = Math.min(replicationCount, clusterSize);

      List<Address> results = new ArrayList<Address>(numCopiesToFind);

      int copyNumber = 0;

      while (results.size() < numCopiesToFind) {
         // we mod the index the 2nd time to make sure the index starts again from the beginning when it reaches the end.
         // e.g., in a cluster of 10 with 3 copies of data, and a key that maps to node index 9, the next 2 backups should
         // be at indexes 0 and 1.

         int index = ((hash % clusterSize) + copyNumber) % clusterSize;
         Address candidate = addresses.get(index);
         results.add(candidate);
         copyNumber++;
      }

      return results;
   }

   public Map<Object, List<Address>> locateAll(Collection<Object> keys, int replCount) {
      Map<Object, List<Address>> locations = new HashMap<Object, List<Address>>();
      for (Object k : keys) locations.put(k, locate(k, replCount));
      return locations;
   }
}
