package org.infinispan.query.affinity;

import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.transport.Address;

/**
 * ShardAllocatorManager is responsible for the mapping between index shards and Infinispan segments for all
 * indexes in a cache.
 *
 * @since 9.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface ShardAllocatorManager {

   /**
    * @return the shard name for a certain segment.
    */
   String getShardFromSegment(int segment);

   /**
    * @return the list of all shards available.
    */
   Set<String> getShards();

   /**
    * @return the list of shards used to do modifications to the index for a given address.
    */
   Set<String> getShardsForModification(Address address);

   /**
    * @return Owner of an index shard given a certain {@link ConsistentHash}.
    */
   boolean isOwnershipChanged(TopologyChangedEvent<?, ?> tce, String shardId);

   /**
    * @return Owner of an index shard.
    */
   Address getOwner(String shardId);

   /*
    * @return ShardId where a certain key belongs to
    */
   String getShardFromKey(Object key);

   /**
    * Initializes the {@link ShardAllocatorManager} with the configured number of segments and shards.
    */
   void initialize(int numberOfShards, int numSegments);
}
