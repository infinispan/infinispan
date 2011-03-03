package org.infinispan.distribution.ch;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A consistent hash algorithm implementation.  Implementations would typically be constructed via reflection so should
 * implement a public, no-arg constructor.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface ConsistentHash {

   /**
    * Sets the collection of cache addresses in the cluster.  The implementation should store these internally and use
    * these to locate keys.
    *
    * @param caches A set of unique caches in cluster.
    */
   void setCaches(Set<Address> caches);

   /**
    * Sets cluster topology information that can be used by CH to improve fault tolerance by smart choosing of backups.
    * More about it <a href="http://community.jboss.org/wiki/DesigningServerHinting">here<a/>
    */
   void setTopologyInfo(TopologyInfo topologyInfo);

   /**
    * Should return a collection of cache addresses in the cluster.
    *
    * @return set of unique of cache addresses
    */
   Set<Address> getCaches();

   /**
    * Locates a key, given a replication count (number of copies).
    *
    * @param key       key to locate
    * @param replCount replication count (number of copies)
    * @return a list of addresses where the key resides, where this list is a subset of the addresses set in {@link
    *         #setCaches(java.util.Set)}.  Should never be null, and should contain replCount elements or the max
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
    * Test to see whether a key is mapped to a given address.
    * @param a address to test
    * @param key key to test
    * @param replCount repl count
    * @return true if the key is mapped to the address; false otherwise
    */
   boolean isKeyLocalToAddress(Address a, Object key, int replCount);

   /**
    * Returns the value between 0 and the hash space limit, or hash id, for a particular address. If there's no such
    * value for an address, this method will return -1.
    *
    * @return An int between 0 and hash space if the address is present in the hash wheel, otherwise it returns -1.
    */
   int getHashId(Address a);

   /**
    * Returns the hash space constant for this consistent hash algorithm class. This integer is often used as modulus
    * for arithmetic operations within the algorithm, for example, limiting the range of possible hash values.
    * 
    * @return A positive integer containing the hash space constant or 0 is not supported by implementation. 
    */
   int getHashSpace();

   /**
    * Returns the nodes that need will replicate their state if the specified node crashes. The return collection
    * should contain all the nodes that backup-ed on leaver and one of the nodes which acted as a backup for the leaver .
    * <p>
    * Pre: leaver must be present in the caches known to this CH, as returned by {@link #getCaches()}
    * @param leaver the node that leaves the cluster
    * @param replCount
    */
   List<Address> getStateProvidersOnLeave(Address leaver, int replCount);

   /**
    * Returns the nodes that would act as state providers when a new node joins:
    * - the nodes for which the joiner is a backup
    * - the nodes that held joiner's state
    */
   List<Address> getStateProvidersOnJoin(Address joiner, int replCount);

   /**
    * Returns the nodes that backup data for the supplied node including the node itself.
    */
   List<Address> getBackupsForNode(Address node, int replCount);
}
