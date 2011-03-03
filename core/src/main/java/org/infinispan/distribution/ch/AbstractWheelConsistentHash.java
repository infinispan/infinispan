package org.infinispan.distribution.ch;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.hash.Hash;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Abstract class for the wheel-based CH implementations.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public abstract class AbstractWheelConsistentHash extends AbstractConsistentHash {

   protected final Log log;
   protected final boolean trace;
   protected SortedMap<Integer, Address> positions;
   // TODO: Maybe address and addressToHashIds can be combined in a LinkedHashMap?
   protected Map<Address, Integer> addressToHashIds;
   protected Hash hashFunction;

   final static int HASH_SPACE = 10240; // no more than 10k nodes?

   protected AbstractWheelConsistentHash() {
      log = LogFactory.getLog(getClass());
      trace = log.isTraceEnabled();
   }

   public void setHashFunction(Hash h) {
      hashFunction = h;
   }

   @Override
   public void setCaches(Set<Address> newCaches) {
      caches = new LinkedHashSet<Address>(newCaches.size());

      positions = new TreeMap<Integer, Address>();
      addressToHashIds = new HashMap<Address, Integer>();

      for (Address a : newCaches) {
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

      // reorder addresses as per the positions.
      caches.addAll(positions.values());
   }

   @Override
   public List<Address> getBackupsForNode(Address node, int replCount) {
      return locate(node, replCount);
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
         return hashId;
   }

   public int getNormalizedHash(Object key) {
      // more efficient impl
      int keyHashCode = hashFunction.hash(key);
      if (keyHashCode == Integer.MIN_VALUE) keyHashCode += 1;
      return Math.abs(keyHashCode) % HASH_SPACE;
   }

   @Override
   public String toString() {
      return "AbstractWheelConsistentHash{" +
            "addresses=" + caches +
            ", topologyInfo=" + topologyInfo +
            ", positions=" + positions +
            ", addressToHashIds=" + addressToHashIds +
            "}";
   }

   public static abstract class Externalizer<T extends AbstractWheelConsistentHash> extends AbstractExternalizer<T> {

      protected abstract T instance();

      @Override
      public void writeObject(ObjectOutput output, T abstractWheelConsistentHash) throws IOException {
         output.writeObject(abstractWheelConsistentHash.hashFunction.getClass().getName());
         output.writeObject(abstractWheelConsistentHash.caches);
         output.writeObject(abstractWheelConsistentHash.positions);
         output.writeObject(abstractWheelConsistentHash.addressToHashIds);
      }

      @Override
      @SuppressWarnings("unchecked")
      public T readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         T abstractWheelConsistentHash = instance();
         String hashFuctionName = (String) unmarshaller.readObject();
         abstractWheelConsistentHash.setHashFunction((Hash) Util.getInstance(hashFuctionName));
         abstractWheelConsistentHash.caches = (Set<Address>) unmarshaller.readObject();
         abstractWheelConsistentHash.positions = (SortedMap<Integer, Address>) unmarshaller.readObject();
         abstractWheelConsistentHash.addressToHashIds = (Map<Address, Integer>) unmarshaller.readObject();
         return abstractWheelConsistentHash;
      }
   }
}
