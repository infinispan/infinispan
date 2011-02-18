package org.infinispan.distribution;

import java.util.Collection;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

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
	public void addRequestor(Object key, Address requestor);

	/**
	 * Flushes a cache (using unicast or multicast) for a set of keys
	 */
	public NotifyingNotifiableFuture<Object> flushCache(Collection<Object> keys,
	      Object retval, Address origin);

}
