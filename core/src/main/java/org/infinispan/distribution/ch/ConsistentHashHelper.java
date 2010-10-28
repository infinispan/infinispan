package org.infinispan.distribution.ch;

import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

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
         ConsistentHash newCH = (ConsistentHash) Util.getInstance(c.getConsistentHashClass());
         List<Address> caches = new ArrayList<Address>(ch.getCaches());
         caches.remove(toRemove);
         newCH.setCaches(caches);
         newCH.setTopologyInfo(topologyInfo);
         return newCH;
      }
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
   public static ConsistentHash createConsistentHash(Configuration c, List<Address> addresses, TopologyInfo topologyInfo) {
      ConsistentHash ch = (ConsistentHash) Util.getInstance(c.getConsistentHashClass());
      ch.setCaches(addresses);
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
   public static ConsistentHash createConsistentHash(Configuration c, List<Address> addresses, TopologyInfo topologyInfo, Address... moreAddresses) {
      List<Address> list = new LinkedList<Address>(addresses);
      list.addAll(Arrays.asList(moreAddresses));
      return createConsistentHash(c, list, topologyInfo);
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
   public static ConsistentHash createConsistentHash(Configuration c, List<Address> addresses, Collection<Address> moreAddresses, TopologyInfo topologyInfo) {
      List<Address> list = new LinkedList<Address>(addresses);
      list.addAll(moreAddresses);
      return createConsistentHash(c, list, topologyInfo);
   }

   /**
    * Creates a new consistent hash instance based on the type specified, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param clazz     type of the consistent hash to create
    * @param addresses with which to populate the consistent hash
    * @param topologyInfo
    * @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(Class<? extends ConsistentHash> clazz, List<Address> addresses, TopologyInfo topologyInfo) {
      ConsistentHash ch;
      ch = Util.getInstance(clazz);
      if (addresses != null && !addresses.isEmpty())  ch.setCaches(addresses);
      ch.setTopologyInfo(topologyInfo);
      return ch;
   }

   /**
    * Creates a new consistent hash instance based on the type specified, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param clazz         type of the consistent hash to create
    * @param addresses     with which to populate the consistent hash
    * @param topologyInfo
    *@param moreAddresses to add to the list of addresses  @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(Class<? extends ConsistentHash> clazz, List<Address> addresses, TopologyInfo topologyInfo, Address... moreAddresses) {
      List<Address> list = new LinkedList<Address>(addresses);
      list.addAll(Arrays.asList(moreAddresses));
      return createConsistentHash(clazz, list, topologyInfo);
   }

   /**
    * Creates a new consistent hash instance based on the type specified, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param clazz         type of the consistent hash to create
    * @param addresses     with which to populate the consistent hash
    * @param moreAddresses to add to the list of addresses
    * @param topologyInfo
    * @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(Class<? extends ConsistentHash> clazz, List<Address> addresses, Collection<Address> moreAddresses, TopologyInfo topologyInfo) {
      List<Address> list = new LinkedList<Address>(addresses);
      list.addAll(moreAddresses);
      return createConsistentHash(clazz, list, topologyInfo);
   }
}
