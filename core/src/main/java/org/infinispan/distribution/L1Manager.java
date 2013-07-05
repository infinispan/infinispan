package org.infinispan.distribution;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
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
}
