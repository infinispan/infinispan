package org.infinispan.distribution;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class DefaultConsistentHash implements ConsistentHash {
   private SortedMap<Integer, Address> caches = new TreeMap<Integer, Address>();
   // must be > max number of nodes in a cluster.  Assume no more than a million nodes in a cluster?  :-)
   private final static int HASH_SPACE = 1000000;


   public void setCaches(Collection<Address> caches) {
      this.caches.clear();
      // evenly distribute the caches across this space.
      int increaseFactor = HASH_SPACE / caches.size();
      int nextIndex = increaseFactor;
      for (Address a: caches) {
         this.caches.put(nextIndex, a);
         nextIndex += increaseFactor;
      }
   }

   public List<Address> locate(Object key, int replicationCount) {
      int hash = Math.abs(key.hashCode());
      int index = hash % HASH_SPACE;

      Set<Address> results = new LinkedHashSet<Address>();

      SortedMap<Integer, Address> tailmap = caches.tailMap(index);
      int count = 0;

      for (Map.Entry<Integer, Address> entry : tailmap.entrySet()) {
         Address val = entry.getValue();
         results.add(val);
         if (++count >= replicationCount)
            break;
      }

      if (count < replicationCount) {
         for (Map.Entry<Integer, Address> entry : caches.entrySet()) {
            Address val = entry.getValue();
            results.add(val);
            if (++count >= replicationCount)
               break;
         }
      }

      return Immutables.immutableListConvert(results);
   }

   public Map<Object, List<Address>> locate(Collection<Object> keys, int replCount) {
      Map<Object, List<Address>> locations = new HashMap<Object, List<Address>>();
      for (Object k: keys) locations.put(k, locate(k, replCount));
      return locations;
   }
}
