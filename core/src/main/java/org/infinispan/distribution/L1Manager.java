package org.infinispan.distribution;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.distribution.L1WriteSynchronizer;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.concurrent.Future;

/**
 * Manages the L1 Cache, in particular recording anyone who is going to cache an
 * a command that a node responds to so that a unicast invalidation can be sent
 * later if needed.
 *
 * @author Pete Muir
 *
 */
@Scope(Scopes.NAMED_CACHE)
public interface L1Manager {

	/**
	 * Records a request that will be cached in another nodes L1
	 */
	void addRequestor(Object key, Address requestor);

   Future<Object> flushCacheWithSimpleFuture(Collection<Object> keys, Object retval, Address origin,
                                             boolean assumeOriginKeptEntryInL1);

   Future<Object> flushCache(Collection<Object> key, Address origin, boolean assumeOriginKeptEntryInL1);

   /**
    * Registers the given write synchronizer to be notified whenever a remote value is looked up for the given key.
    * If the synchronizer is no longer needed to be signaled, the user should unregister it using
    * {#unregisterL1WriteSynchronizer}
    * @param key The key that is desired to trigger the synchronizer
    * @param sync The synchronizer to update
    * @throws IllegalStateException This is thrown if there is already a synchronizer associated with the L1 Manager.
    */
   void registerL1WriteSynchronizer(Object key, L1WriteSynchronizer sync);

   /**
    * Unregister the given write synchronizer if present.
    * @param key The key to unregister the given synchronizer if present.
    */
   void unregisterL1WriteSynchronizer(Object key);
}
