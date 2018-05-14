package org.infinispan.conflict.impl;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.conflict.ConflictManager;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

/**
 * @author Ryan Emerson
 * @since 9.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface InternalConflictManager<K, V> extends ConflictManager<K, V> {
   void cancelVersionRequests();
   void restartVersionRequests();
   void cancelConflictResolution();
   CompletableFuture<Void> resolveConflicts(CacheTopology cacheTopology, Set<Address> preferredNodes);
}
