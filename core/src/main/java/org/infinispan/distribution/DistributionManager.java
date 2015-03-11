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
 * @author Pedro Ruivo
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DistributionManager {

   /**
    * Returns the data locality characteristics of a given key.
    *
    * @param key        key to test.
    * @param lookupMode specifies if the lookup is for read or write.
    * @return a {@link org.infinispan.distribution.DataLocality} that allows you to test whether a key is mapped to the
    * local node or not, and the degree of certainty of such a result.
    */
   DataLocality getLocality(Object key, LookupMode lookupMode);

   /**
    * Locates a key in a cluster.  The returned addresses <i>may not</i> be owners of the keys if a rehash happens to be
    * in progress or is pending, so when querying these servers, invalid responses should be checked for and the next
    * address checked accordingly.
    *
    * @param key        key to test.
    * @param lookupMode specifies if the lookup is for read or write.
    * @return a list of addresses where the key may reside
    */
   List<Address> locate(Object key, LookupMode lookupMode);

   /**
    * Returns the first Address containing the key. Equivalent to returning the first element of {@link #locate(Object,
    * LookupMode)}
    *
    * @param key        key to test.
    * @param lookupMode specifies if the lookup is for read or write.
    * @return the first address on which the key may reside
    */
   Address getPrimaryLocation(Object key, LookupMode lookupMode);

   /**
    * Locates a list of keys in a cluster.  Like {@link #locate(Object, LookupMode)} the returned addresses <i>may
    * not</i> be owners of the keys if a rehash happens to be in progress or is pending, so when querying these servers,
    * invalid responses should be checked for and the next address checked accordingly.
    *
    * @param keys       list of keys to locate.
    * @param lookupMode specifies if the lookup is for read or write.
    * @return a list of addresses where the keys reside.
    */
   Set<Address> locateAll(Collection<Object> keys, LookupMode lookupMode);

   /**
    * It returns the {@link org.infinispan.distribution.ch.ConsistentHash} used for reads.
    * <p/>
    * When not rehash is in place, it returns the same consistent hash as {@link #getWriteConsistentHash()}.
    *
    * @return the {@link org.infinispan.distribution.ch.ConsistentHash} used for reads.
    */
   ConsistentHash getReadConsistentHash();

   /**
    * It returns the {@link org.infinispan.distribution.ch.ConsistentHash} used for writes.
    * <p/>
    * When not rehash is in place, it returns the same consistent hash as {@link #getReadConsistentHash()}.
    *
    * @return the {@link org.infinispan.distribution.ch.ConsistentHash} used for writes.
    */
   ConsistentHash getWriteConsistentHash();

   /**
    * Tests whether a given key is affected by a rehash that may be in progress.  If no rehash is in progress, this
    * method returns {@code false}. Helps determine whether additional steps are necessary in handling an operation with
    * a given key.
    *
    * @param key key to test
    * @return whether a key is affected by a rehash
    */
   boolean isAffectedByRehash(Object key);

   /**
    * Tests whether a rehash is in progress
    *
    * @return {@code true} if a rehash is in progress, {@code false} otherwise
    */
   boolean isRehashInProgress();

   /**
    * Tests whether the current instance has completed joining the cluster
    *
    * @return {@code true} if node is joined, {@code false} otherwise
    */
   boolean isJoinComplete();
}

