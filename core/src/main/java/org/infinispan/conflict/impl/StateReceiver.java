package org.infinispan.conflict.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateChunk;

/**
 * @author Ryan Emerson
 * @since 9.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateReceiver<K, V> {

   /**
    * Cancels all ongoing replica requests.
    */
   void cancelRequests();

   /**
    * Return all replicas of a cache entry for a given segment. We require the ConsitentHash to be passed here, as it is
    * necessary for the hash of the last stable topology to be utilised during an automatic merge, before a
    * new merged topology is installed.
    *
    * @throws IllegalStateException if this method is invoked whilst a previous request for Replicas is still executing
    */
   CompletableFuture<List<Map<Address, CacheEntry<K, V>>>> getAllReplicasForSegment(int segmentId, LocalizedCacheTopology topology, long timeout);

   void receiveState(Address sender, int topologyId, Collection<StateChunk> stateChunks);
}
