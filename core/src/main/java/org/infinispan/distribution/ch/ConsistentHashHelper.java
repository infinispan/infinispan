package org.infinispan.distribution.ch;

import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.hash.Hash;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A helper class that handles the construction of consistent hash instances based on configuration.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ConsistentHashHelper {

   /**                                 
    * Returns a new consistent hash of the same type with the given address removed.
    *
    * @param ch       consistent hash to start with
    * @param toRemove address to remove
    * @param c        configuration
    * @param topologyInfo
    * @return a new consistent hash instance of the same type
    */
   public static ConsistentHash removeAddress(ConsistentHash ch, Address toRemove, Configuration c, TopologyInfo topologyInfo) {
      if (ch instanceof UnionConsistentHash)
         return removeAddressFromUnionConsistentHash((UnionConsistentHash) ch, toRemove, c, topologyInfo);
      else {
         ConsistentHash newCH = constructConsistentHashInstance(c);
         Set<Address> caches = new HashSet<Address>(ch.getCaches());
         caches.remove(toRemove);
         newCH.setCaches(caches);
         newCH.setTopologyInfo(topologyInfo);
         return newCH;
      }
   }

   private static ConsistentHash constructConsistentHashInstance(Configuration c) {
      ConsistentHash ch = (ConsistentHash) Util.getInstance(c.getConsistentHashClass());
      if (ch instanceof AbstractWheelConsistentHash) {
         Hash h = (Hash) Util.getInstance(c.getHashFunctionClass());
         ((AbstractWheelConsistentHash) ch).setHashFunction(h);
      }
      return ch;
   }

   private static ConsistentHash constructConsistentHashInstance(Class<? extends ConsistentHash> clazz, Hash hash) {
      ConsistentHash ch = Util.getInstance(clazz);
      if (ch instanceof AbstractWheelConsistentHash) {
         ((AbstractWheelConsistentHash) ch).setHashFunction(hash);
      }
      return ch;
   }

   /**
    * Creates a new UnionConsistentHash instance based on the old instance, removing the provided address from both
    * target consistent hash instances in the union.
    *
    * @param uch      union consistent hash instance
    * @param toRemove address to remove
    * @param c        configuration
    * @param topologyInfo
    * @return a new UnionConsistentHash instance
    */
   public static UnionConsistentHash removeAddressFromUnionConsistentHash(UnionConsistentHash uch, Address toRemove, Configuration c, TopologyInfo topologyInfo) {
      ConsistentHash newFirstCH = removeAddress(uch.getOldConsistentHash(), toRemove, c, topologyInfo);
      ConsistentHash newSecondCH = removeAddress(uch.getNewConsistentHash(), toRemove, c, topologyInfo);
      return new UnionConsistentHash(newFirstCH, newSecondCH);
   }

   /**
    * Creates a new consistent hash instance based on the user's configuration, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param c         configuration
    * @param addresses with which to populate the consistent hash
    * @param topologyInfo
    * @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(Configuration c, Collection<Address> addresses, TopologyInfo topologyInfo) {
      ConsistentHash ch = constructConsistentHashInstance(c);
      ch.setCaches(toSet(addresses));
      ch.setTopologyInfo(topologyInfo);
      return ch;
   }

   /**
    * Creates a new consistent hash instance based on the user's configuration, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param c             configuration
    * @param addresses     with which to populate the consistent hash
    * @param topologyInfo
    *@param moreAddresses to add to the list of addresses  @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(Configuration c, Collection<Address> addresses, TopologyInfo topologyInfo, Address... moreAddresses) {
      Set<Address> caches = new HashSet<Address>(addresses);
      caches.addAll(Arrays.asList(moreAddresses));
      return createConsistentHash(c, caches, topologyInfo);
   }

   /**
    * Creates a new consistent hash instance based on the user's configuration, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param c             configuration
    * @param addresses     with which to populate the consistent hash
    * @param moreAddresses to add to the list of addresses
    * @param topologyInfo
    * @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(Configuration c, Collection<Address> addresses, Collection<Address> moreAddresses, TopologyInfo topologyInfo) {
      Set<Address> caches = new HashSet<Address>(addresses);
      caches.addAll(moreAddresses);
      return createConsistentHash(c, caches, topologyInfo);
   }

   /**
    * Creates a new consistent hash instance based on the type specified, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param template An older consistent hash instance to clone
    * @param addresses with which to populate the consistent hash
    * @param topologyInfo
    * @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(ConsistentHash template, Collection<Address> addresses, TopologyInfo topologyInfo) {
      Hash hf = null;
      if (template instanceof AbstractWheelConsistentHash) {
         hf = ((AbstractWheelConsistentHash) template).hashFunction;
      }
      ConsistentHash ch = constructConsistentHashInstance(template.getClass(), hf);
      if (addresses != null && !addresses.isEmpty())  ch.setCaches(toSet(addresses));
      ch.setTopologyInfo(topologyInfo);
      return ch;
   }

   /**
    * Creates a new consistent hash instance based on the type specified, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param template  An older consistent hash instance to clone
    * @param addresses     with which to populate the consistent hash
    * @param topologyInfo
    *@param moreAddresses to add to the list of addresses  @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(ConsistentHash template, Collection<Address> addresses, TopologyInfo topologyInfo, Address... moreAddresses) {
      Set<Address> caches = new HashSet<Address>(addresses);
      caches.addAll(Arrays.asList(moreAddresses));
      return createConsistentHash(template, caches, topologyInfo);
   }

   private static Set<Address> toSet(Collection<Address> c) {
      if (c instanceof Set) return (Set<Address>) c;
      return new HashSet<Address>(c);
   }
}
