package org.infinispan.distribution;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A component that manages the distribution of elements across a cache cluster
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
 * @author anistor@redhat.com
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DistributionManager {

   /**
    * Returns the data locality characteristics of a given key.
    * @param key key to test
    * @return a DataLocality that allows you to test whether a key is mapped to the local node or not, and the degree of
    * certainty of such a result.
    */
   DataLocality getLocality(Object key); //todo [anistor] this has to take an additional parameter that specifies if the lookup is for read or write

   /**
    * Locates a key in a cluster.  The returned addresses <i>may not</i> be owners of the keys if a rehash happens to be
    * in progress or is pending, so when querying these servers, invalid responses should be checked for and the next
    * address checked accordingly.
    *
    * @param key key to test
    * @return a list of addresses where the key may reside
    */
   List<Address> locate(Object key); //todo [anistor] this has to take an additional parameter that specifies if the lookup is for read or write

   /**
    * Returns the first Address containing the key.  Equivalent to returning the first element of {@link #locate(Object)}
    * @param key key to test
    * @return the first address on which the key may reside
    */
   Address getPrimaryLocation(Object key);  //todo [anistor] this has to take an additional parameter that specifies if the lookup is for read or write

   /**
    * Locates a list of keys in a cluster.  Like {@link #locate(Object)} the returned addresses <i>may not</i> be owners
    * of the keys if a rehash happens to be in progress or is pending, so when querying these servers, invalid responses
    * should be checked for and the next address checked accordingly.
    *
    * @param keys list of keys to locate
    * @return a list of addresses where the keys reside
    */
   Set<Address> locateAll(Collection<Object> keys); //todo [anistor] this has to take an additional parameter that specifies if the lookup is for read or write

   /**
    * Retrieves the consistent hash instance currently in use, an instance of the configured ConsistentHash
    * class (which defaults to {@link org.infinispan.distribution.ch.impl.DefaultConsistentHash}.
    *
    * @return a ConsistentHash instance
    */
   ConsistentHash getConsistentHash();

   ConsistentHash getReadConsistentHash();

   ConsistentHash getWriteConsistentHash();

   /**
    * Tests whether a given key is affected by a rehash that may be in progress.  If no rehash is in progress, this method
    * returns false.  Helps determine whether additional steps are necessary in handling an operation with a given key.
    *
    * @param key key to test
    * @return whether a key is affected by a rehash
    */
   boolean isAffectedByRehash(Object key);

   /**
    * Tests whether a rehash is in progress
    * @return true if a rehash is in progress, false otherwise
    */
   boolean isRehashInProgress();

   /**
    * Tests whether the current instance has completed joining the cluster
    * @return true if join is in progress, false otherwise
    */
   boolean isJoinComplete();
}

