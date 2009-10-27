package org.infinispan.distribution;

import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ConsistentHashHelper {

   /**
    * Returns a new consistent hash of the same type with the given address removed.
    *
    * @param ch       consistent hash to start with
    * @param toRemove address to remove
    * @param c        configuration
    * @return a new consistent hash instance of the same type
    */
   public static ConsistentHash removeAddress(ConsistentHash ch, Address toRemove, Configuration c) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
      if (ch instanceof UnionConsistentHash)
         return removeAddressFromUnionConsistentHash((UnionConsistentHash) ch, toRemove, c);
      else {
         ConsistentHash newCH = (ConsistentHash) Util.getInstance(c.getConsistentHashClass());
         List<Address> caches = ch.getCaches();
         caches.remove(toRemove);
         newCH.setCaches(caches);
         return newCH;
      }
   }

   public static UnionConsistentHash removeAddressFromUnionConsistentHash(UnionConsistentHash uch, Address toRemove, Configuration c) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
      ConsistentHash newFirstCH = removeAddress(uch.getOldConsistentHash(), toRemove, c);
      ConsistentHash newSecondCH = removeAddress(uch.getNewConsistentHash(), toRemove, c);
      return new UnionConsistentHash(newFirstCH, newSecondCH);
   }

   public static ConsistentHash createConsistentHash(Configuration c, List<Address> addresses) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
      ConsistentHash ch = (ConsistentHash) Util.getInstance(c.getConsistentHashClass());
      ch.setCaches(addresses);
      return ch;
   }

   public static ConsistentHash createConsistentHash(Configuration c, List<Address> addresses, Address... moreAddresses) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
      List<Address> list = new LinkedList<Address>(addresses);
      list.addAll(Arrays.asList(moreAddresses));
      return createConsistentHash(c, list);
   }

   public static ConsistentHash createConsistentHash(Configuration c, List<Address> addresses, Collection<Address> moreAddresses) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
      List<Address> list = new LinkedList<Address>(addresses);
      list.addAll(moreAddresses);
      return createConsistentHash(c, list);
   }

   public static ConsistentHash createConsistentHash(Class<? extends ConsistentHash> clazz, List<Address> addresses) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
      ConsistentHash ch = Util.getInstance(clazz);
      if (addresses != null && addresses.size() > 0) ch.setCaches(addresses);
      return ch;
   }

   public static ConsistentHash createConsistentHash(Class<? extends ConsistentHash> clazz, List<Address> addresses, Address... moreAddresses) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
      List<Address> list = new LinkedList<Address>(addresses);
      list.addAll(Arrays.asList(moreAddresses));
      return createConsistentHash(clazz, list);
   }

   public static ConsistentHash createConsistentHash(Class<? extends ConsistentHash> clazz, List<Address> addresses, Collection<Address> moreAddresses) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
      List<Address> list = new LinkedList<Address>(addresses);
      list.addAll(moreAddresses);
      return createConsistentHash(clazz, list);
   }
}
