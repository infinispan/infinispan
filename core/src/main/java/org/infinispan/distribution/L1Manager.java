package org.infinispan.distribution;

import java.util.Collection;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

@Scope(Scopes.NAMED_CACHE)
public interface L1Manager {
	
	public void addRequestor(Object key, Address requestor);
	
	public NotifyingNotifiableFuture<Object> flushCache(int numCallRecipients, Collection<Object> keys, Object retval);
	
	public NotifyingNotifiableFuture<Object> flushLocalCache(Collection<Object> keys);

}
