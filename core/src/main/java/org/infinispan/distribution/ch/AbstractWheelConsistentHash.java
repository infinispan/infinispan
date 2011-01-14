package org.infinispan.distribution.ch;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.hash.Hash;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.ReplCache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Abstract class for the wheel-based CH implementations.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public abstract class AbstractWheelConsistentHash extends AbstractConsistentHash {

   private static Log log = LogFactory.getLog(AbstractWheelConsistentHash.class);
   protected ArrayList<Address> addresses;
   protected SortedMap<Integer, Address> positions;
   // TODO: Maybe address and addressToHashIds can be combined in a LinkedHashMap?
   protected Map<Address, Integer> addressToHashIds;
   protected Hash hashFunction;

   final static int HASH_SPACE = 10240; // no more than 10k nodes?

   public void setHashFunction(Hash h) {
      hashFunction = h;
   }

   public void setCaches(List<Address> caches) {
      super.setCaches(caches);

      addresses = new ArrayList<Address>(caches);

      // this list won't grow.
      addresses.trimToSize();

      positions = new TreeMap<Integer, Address>();
      addressToHashIds = new HashMap<Address, Integer>();

      for (Address a : addresses) {
         int positionIndex = Math.abs(hashFunction.hash(a)) % HASH_SPACE;
         // this is deterministic since the address list is ordered and the order is consistent across the grid
         while (positions.containsKey(positionIndex)) positionIndex = positionIndex + 1 % HASH_SPACE;
         positions.put(positionIndex, a);
         // If address appears several times, take the lowest value to guarantee that
         // at least the initial value and subsequent +1 values would end up in the same node
         // TODO: Remove this check since https://jira.jboss.org/jira/browse/ISPN-428 contains a proper fix for this
         if (!addressToHashIds.containsKey(a))
            addressToHashIds.put(a, positionIndex);
      }

      addresses.clear();
      // reorder addresses as per the positions.
      for (Address a : positions.values()) addresses.add(a);
      if (log.isTraceEnabled()) {
         log.trace("Position are: " + positions);
      }
   }

   @Override
   public List<Address> getBackupsForNode(Address node, int replCount) {
      return locate(node, replCount);
   }

   public List<Address> getCaches() {
      return addresses;
   }

   @Override
   public int getHashSpace() {
      return HASH_SPACE;
   }


   @Override
   public int getHashId(Address a) {
      Integer hashId = addressToHashIds.get(a);
      if (hashId == null)
         return -1;
      else
         return hashId.intValue();
   }

   protected int getNormalizedHash(Object key) {
      // more efficient impl
      int keyHashCode = hashFunction.hash(key);
      if (keyHashCode == Integer.MIN_VALUE) keyHashCode += 1;
      return Math.abs(keyHashCode) % HASH_SPACE;
   }

   @Override
   public String toString() {
      return "AbstractWheelConsistentHash{" +
            "addresses=" + addresses +
            ", positions=" + positions +
            ", addressToHashIds=" + addressToHashIds +
            "} " + super.toString();
   }
}
