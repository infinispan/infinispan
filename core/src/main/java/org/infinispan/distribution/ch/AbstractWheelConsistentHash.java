package org.infinispan.distribution.ch;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.infinispan.util.hash.MurmurHash2.hash;

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

   final static int HASH_SPACE = 10240; // no more than 10k nodes?

   public void setCaches(List<Address> caches) {
      super.setCaches(caches);

      addresses = new ArrayList<Address>(caches);

      // this list won't grow.
      addresses.trimToSize();

      positions = new TreeMap<Integer, Address>();
      addressToHashIds = new HashMap<Address, Integer>();

      for (Address a : addresses) {
         int positionIndex = Math.abs(hash(a)) % HASH_SPACE;
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

   public boolean isStateReceiverOnLeave(Address leaver, Address node, int replCount) {
      for (Address address : addresses) {
         List<Address> backups = locate(address, replCount + 1);
         if (backups.contains(leaver) && (backups.indexOf(node) == backups.size() - 1)) {
            return true;
         }
      }
      return false;
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
      int keyHashCode = hash(key);
      if (keyHashCode == Integer.MIN_VALUE) keyHashCode += 1;
      return Math.abs(keyHashCode) % HASH_SPACE;
   }
}
