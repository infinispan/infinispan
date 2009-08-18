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
   void setCaches(Collection<Address> caches);

   /**
    * Should return a collection of cache addresses in the cluster.
    *
    * @return collection of cache addresses
    */
   Collection<Address> getCaches();

   /**
    * Locates a key, given a replication count (number of copies).
    *
    * @param key       key to locate
    * @param replCount replication count (number of copies)
    * @return a list of addresses where the key resides, where this list is a subset of the addresses set in {@link
    *         #setCaches(java.util.Collection)}.  Should never be null, and should contain replCount elements or the max
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
    * Tests whether a group of addresses are in the same subspace of the hash space.  Addresses are in the same subspace
    * if an arbitrary key mapped to one address could also be mapped to the other.
    *
    * @param a1 address to test
    * @param a2 address to test
    * @return true of the two addresses are in the same subspace, false otherwise.
    */
   boolean isInSameSubspace(Address a1, Address a2);
}
