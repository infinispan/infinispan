package org.infinispan.distribution.ch;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.hash.Hash;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * <p>
 * Abstract class for the wheel-based CH implementations.
 * </p>
 * 
 * <p>
 * This base class supports consistent hashses which wish to enable virtual nodes. To do this
 * the implementation should override {@link #isVirtualNodesEnabled()} and return true when
 * virtual nodes should be enabled (we recommend at least that {@link #numVirtualNodes} should be
 * > 1 for virtual nodes to be enabled!). It is assumed that implementations of a consistent hash
 * in which virtual nodes are enabled will take responsibility for ensuring that the fact virtual
 * nodes are in use does not escape the consistent hash implementation.
 * </p> 
 * 
 * <p>
 * The only consistent hash in Inifinispan to provide virtual node support is the topology aware
 * consistent hash,
 * </p>
 * 
 * <p>
 * In order to do this, the
 * implementation can use {@link #getRealAddress(Address)}, {@link #getRealAddresses(List)} and 
 * {@link #getRealAddresses(Set)} to convert the virtual addresses obtained from {@link #positions},
 * {@link #addressToHashIds} and {@link AbstractConsistentHash#caches} to real addresses. In particular an 
 * implementation should ensure that {@link #locate(Object, int)}, {@link #getStateProvidersOnLeave(Address, int)}
 * and {@link #getStateProvidersOnJoin(Address, int)} do not return virtual addresses (as implementations of
 * these methods are not provided by this abstract super class). The behavior of Infinispan if 
 * virtual addresses leak from the consistent hash implementation is not tested.
 * </p>
 *
 * @author Mircea.Markus@jboss.com
 * @author Pete Muir
 * @since 4.2
 */
public abstract class AbstractWheelConsistentHash extends AbstractConsistentHash {

   protected final Log log;
   protected final boolean trace;
   protected SortedMap<Integer, Address> positions;
   // TODO: Maybe address and addressToHashIds can be combined in a LinkedHashMap?
   protected Map<Address, Integer> addressToHashIds;
   protected Hash hashFunction;
   protected int numVirtualNodes;

   final static int HASH_SPACE = 10240; // no more than 10k nodes?

   protected AbstractWheelConsistentHash() {
      log = LogFactory.getLog(getClass());
      trace = log.isTraceEnabled();
   }

   public void setHashFunction(Hash h) {
      hashFunction = h;
   }
   
   public void setNumVirtualNodes(Integer numVirtualNodes) {
      this.numVirtualNodes = numVirtualNodes;
   }

   @Override
   public void setCaches(Set<Address> newCaches) {
      caches = new LinkedHashSet<Address>(newCaches.size());

      positions = new TreeMap<Integer, Address>();
      addressToHashIds = new HashMap<Address, Integer>();

      if (trace) log.trace("Adding %s nodes to cluster", newCaches.size());
      
      for (Address a : newCaches) {
         if (isVirtualNodesEnabled()) {
            if (trace) log.trace("Adding %s virtual nodes for real node %s", numVirtualNodes, a);
            for (int i = 0; i < numVirtualNodes; i++) {
               Address va = new VirtualAddress(a, i);
               if (trace) log.trace("Adding virtual node %s", va);
               addNode(va);
            }
         } else {
            if (trace) log.trace("Adding node %s", a);
            addNode(a);
         }
      }

      // reorder addresses as per the positions.
      caches.addAll(positions.values());
   }
   
   @Override
   public Set<Address> getCaches() {
      return getRealAddresses(caches);
   }
   
   protected void addNode(Address a) {
      int positionIndex = Math.abs(hashFunction.hash(a)) % HASH_SPACE;
      // this is deterministic since the address list is ordered and the order is consistent across the grid
      while (positions.containsKey(positionIndex)) positionIndex = positionIndex + 1 % HASH_SPACE;
      positions.put(positionIndex, a);
      // If address appears several times, take the lowest value to guarantee that
      // at least the initial value and subsequent +1 values would end up in the same node
      // TODO: Remove this check since https://jira.jboss.org/jira/browse/ISPN-428 contains a proper fix for this
      if (!addressToHashIds.containsKey(a))
         addressToHashIds.put(a, positionIndex);
      if (trace) log.trace("Added node %s", a);
   }
   
   protected Address getRealAddress(Address a) {
      if (isVirtualNodesEnabled())
         return ((VirtualAddress) a).getRealAddress();
      else
         return a;
   }
   
   protected List<Address> getRealAddresses(List<Address> virtualAddresses) {
      if (virtualAddresses.isEmpty())
         return emptyList();
      else if (virtualAddresses.size() == 1) {
         if (isVirtualNodesEnabled()) {
            VirtualAddress a = (VirtualAddress) virtualAddresses.iterator().next();
            return singletonList(a.getRealAddress());
         } else
            return virtualAddresses;
      } else {
         if (isVirtualNodesEnabled()) {
            List<Address> addresses = new ArrayList<Address>();
            for (Address a : virtualAddresses) {
               VirtualAddress va = (VirtualAddress) a;
               addresses.add(va.getRealAddress());
            }
            return addresses;
         } else
            return virtualAddresses;
      }
   }
   
   protected Set<Address> getRealAddresses(Set<Address> virtualAddresses) {
      if (virtualAddresses.isEmpty())
         return Collections.emptySet();
      else if (virtualAddresses.size() == 1) {
         if (isVirtualNodesEnabled()) {
            VirtualAddress a = (VirtualAddress) virtualAddresses.iterator().next();
            return Collections.singleton(a.getRealAddress());
         } else
            return virtualAddresses;
      } else {
         if (isVirtualNodesEnabled()) {
            Set<Address> addresses = new HashSet<Address>();
            for (Address a : virtualAddresses) {
               VirtualAddress va = (VirtualAddress) a;
               addresses.add(va.getRealAddress());
            }
            return addresses;
         } else
            return virtualAddresses;
      }
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
      return getClass().getSimpleName() + " {" +
            "addresses=" + caches +
            ", positions=" + positions +
            ", addressToHashIds=" + addressToHashIds +
            "}";
   }
   
   protected boolean isVirtualNodesEnabled() {
      return false;
   }

   public static abstract class Externalizer<T extends AbstractWheelConsistentHash> extends AbstractExternalizer<T> {

      protected abstract T instance();

      @Override
      public void writeObject(ObjectOutput output, T abstractWheelConsistentHash) throws IOException {
         output.writeObject(abstractWheelConsistentHash.hashFunction.getClass().getName());
         output.writeObject(abstractWheelConsistentHash.caches);
         output.writeObject(abstractWheelConsistentHash.positions);
         output.writeObject(abstractWheelConsistentHash.addressToHashIds);
         output.writeInt(abstractWheelConsistentHash.numVirtualNodes);
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
         abstractWheelConsistentHash.numVirtualNodes = unmarshaller.readInt();
         return abstractWheelConsistentHash;
      }
   }
}
