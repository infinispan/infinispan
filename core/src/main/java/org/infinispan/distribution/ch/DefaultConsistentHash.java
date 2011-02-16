package org.infinispan.distribution.ch;

import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import static java.lang.Math.min;

public class DefaultConsistentHash extends AbstractWheelConsistentHash {

   public List<Address> locate(Object key, int replCount) {
      int hash = getNormalizedHash(key);
      int numCopiesToFind = min(replCount, addresses.size());

      List<Address> owners = new ArrayList<Address>(numCopiesToFind);

      SortedMap<Integer, Address> candidates = positions.tailMap(hash);

      int numOwnersFound = 0;

      for (Address a : candidates.values()) {
         if (numOwnersFound < numCopiesToFind) {
            owners.add(a);
            numOwnersFound++;
         } else {
            break;
         }
      }

      if (numOwnersFound < numCopiesToFind) {
         for (Address a : positions.values()) {
            if (numOwnersFound < numCopiesToFind) {
               owners.add(a);
               numOwnersFound++;
            } else {
               break;
            }
         }
      }

      return owners;
   }

   @Override
   public boolean isKeyLocalToAddress(Address target, Object key, int replCount) {
      int hash = getNormalizedHash(key);
      int numCopiesToFind = min(replCount, addresses.size());

      SortedMap<Integer, Address> candidates = positions.tailMap(hash);
      int nodesTested = 0;
      for (Address a : candidates.values()) {
         if (nodesTested < numCopiesToFind) {
            if (a.equals(target)) return true;
            nodesTested++;
         } else {
            break;
         }
      }

      // start from the beginning
      if (nodesTested < numCopiesToFind) {
         for (Address a : positions.values()) {
            if (nodesTested < numCopiesToFind) {
               if (a.equals(target)) return true;
               nodesTested++;
            } else {
               break;
            }
         }
      }

      return false;
   }


   public int getDistance(Address a1, Address a2) {
      if (a1 == null || a2 == null) throw new NullPointerException("Cannot deal with nulls as parameters!");

      int p1 = addresses.indexOf(a1);
      if (p1 < 0)
         return -1;

      int p2 = addresses.indexOf(a2);
      if (p2 < 0)
         return -1;

      if (p1 <= p2)
         return p2 - p1;
      else
         return addresses.size() - (p1 - p2);
   }

   public boolean isAdjacent(Address a1, Address a2) {
      int distance = getDistance(a1, a2);
      return distance == 1 || distance == addresses.size() - 1;
   }

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

   public static class Externalizer extends AbstractWheelConsistentHash.Externalizer<DefaultConsistentHash> {
      @Override
      protected DefaultConsistentHash instance() {
         return new DefaultConsistentHash();
      }

      @Override
      public Integer getId() {
         return Ids.DEFAULT_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends DefaultConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends DefaultConsistentHash>>asSet(DefaultConsistentHash.class);
      }
   }

   @Override
   public String toString() {
      return "DefaultConsistentHash{" +
              "addresses =" + positions +
              ", hash space =" + HASH_SPACE +
              '}';
   }

   public List<Address> getAddressOnTheWheel() {
      return addresses;
   }

   public List<Address> getStateProvidersOnJoin(Address self, int replCount) {
      List<Address> l = new LinkedList<Address>();
      List<Address> caches = getCaches();
      int selfIdx = caches.indexOf(self);
      if (selfIdx >= replCount - 1) {
         l.addAll(caches.subList(selfIdx - replCount + 1, selfIdx));
      } else {
         l.addAll(caches.subList(0, selfIdx));
         int alreadyCollected = l.size();
         l.addAll(caches.subList(caches.size() - replCount + 1 + alreadyCollected, caches.size()));
      }

      Address plusOne;
      if (selfIdx == caches.size() - 1)
         plusOne = caches.get(0);
      else
         plusOne = caches.get(selfIdx + 1);

      if (!l.contains(plusOne)) l.add(plusOne);
      return l;
   }

   public List<Address> getStateProvidersOnLeave(Address leaver, int replCount) {
      if (trace) log.trace("List of addresses is: %s. leaver is: %s", addresses, leaver);
      Set<Address> holders = new HashSet<Address>();
      for (Address address : addresses) {
         if (isAdjacent(leaver, address)) {
            holders.add(address);
            if (trace) log.trace("%s is state holder", address);
         } else {
            if (trace) log.trace("%s is NOT state holder", address);
         }
      }
      return new ArrayList<Address>(holders);
   }
}