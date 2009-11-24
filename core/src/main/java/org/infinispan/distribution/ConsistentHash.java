package org.infinispan.distribution;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A consistent hash algorithm implementation.  Implementations would typically be constructed via reflection so should
 * implement a public, no-arg constructor.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ConsistentHash {

   /**
    * Sets the collection of cache addresses in the cluster.  The implementation should store these internally and use
    * these to locate keys.
    *
    * @param caches caches in cluster.
    */
   void setCaches(List<Address> caches);

   /**
    * Should return a collection of cache addresses in the cluster.
    *
    * @return collection of cache addresses
    */
   List<Address> getCaches();

   /**
    * Locates a key, given a replication count (number of copies).
    *
    * @param key       key to locate
    * @param replCount replication count (number of copies)
    * @return a list of addresses where the key resides, where this list is a subset of the addresses set in {@link
    *         #setCaches(java.util.List)}.  Should never be null, and should contain replCount elements or the max
    *         number of caches available, whichever is smaller.
    */
   List<Address> locate(Object key, int replCount);

   /**
    * The logical equivalent of calling {@link #locate(Object, int)} multiple times for each key in the collection of
    * keys. Implementations may be optimised for such a bulk lookup, or may just repeatedly call {@link #locate(Object,
    * int)}.
    *
    * @param keys      keys to locate
    * @param replCount replication count (number of copies) for each key
    * @return Map of locations, keyed on key.
    */
   Map<Object, List<Address>> locateAll(Collection<Object> keys, int replCount);

   /**
    * Calculates the logical distance between two addresses.  This distance is based on where the addresses lie in the
    * hash space.
    *
    * @param a1 address to test
    * @param a2 address to test
    * @return the distance between the 2 nodes.  Always a positive number, where the distance between a1 and itself is
    *         0. The distance between a1 and the next adjacent node is 1 and teh distance between a1 and the previous
    *         adjacent node is caches.size() - 1.  A -1 may be returned if either of the addresses do not exist.
    */
   int getDistance(Address a1, Address a2);

   /**
    * Tests whether two addresses are logically next to each other in the hash space.
    *
    * @param a1 address to test
    * @param a2 address to test
    * @return true if adjacent, false if not
    */
   boolean isAdjacent(Address a1, Address a2);

   /**
    * Test to see whether a key is mapped to a given address.
    * @param a address to test
    * @param key key to test
    * @param replCount repl count
    * @return true if the key is mapped to the address; false otherwise
    */
   boolean isKeyLocalToAddress(Address a, Object key, int replCount);
}
