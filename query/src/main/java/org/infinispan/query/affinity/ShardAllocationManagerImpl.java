package org.infinispan.query.affinity;

import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.Listener.Observation;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.LogFactory;

/**
 * @see ShardAllocatorManager
 * @since 9.0
 */
@Listener(observation = Observation.POST)
public final class ShardAllocationManagerImpl implements ShardAllocatorManager {

   private static final Log logger = LogFactory.getLog(ShardAllocationManagerImpl.class, Log.class);

   private DistributionManager distributionManager;
   private volatile boolean initialized;
   private int numSegments;
   private int numShards;
   private volatile ShardDistribution shardDistribution;

   @Inject
   public void inject(Cache<?, ?> cache, DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
      cache.addListener(this);
   }

   @Override
   public String getShardFromSegment(int segment) {
      String shardId = this.getShardDistribution().getShardFromSegment(segment);
      logger.debugf("ShardId for segment %d: %s", segment, shardId);
      return shardId;
   }

   private ShardDistribution buildShardDistribution(ConsistentHash consistentHash) {
      return ShardDistributionFactory.build(numShards, numSegments, consistentHash);
   }

   private ShardDistribution getShardDistribution() {
      if (shardDistribution == null) {
         shardDistribution = this.buildShardDistribution(
               distributionManager == null ? null : distributionManager.getWriteConsistentHash());
      }
      return shardDistribution;
   }

   @Override
   public Address getOwner(String shardId) {
      return this.getShardDistribution().getOwner(shardId);
   }

   @Override
   public String getShardFromKey(Object key) {
      int segment = distributionManager.getCacheTopology().getSegment(key);
      logger.debugf("Segment for key %s: %d", key, segment);
      return this.getShardDistribution().getShardFromSegment(segment);
   }

   @Override
   public void initialize(int numberOfShards, int numSegments) {
      this.numSegments = numSegments;
      this.numShards = numberOfShards;
      initialized = true;
   }

   @Override
   public Set<String> getShards() {
      Set<String> shards = this.getShardDistribution().getShardsIdentifiers();
      logger.debugf("AllShards:%s", shards);
      return shards;
   }

   @Override
   public Set<String> getShardsForModification(Address address) {
      Set<String> shards = this.getShardDistribution().getShards(address);
      logger.debugf("Shard for modification %s for address %s", shards, address);
      return shards;
   }

   @Override
   public boolean isOwnershipChanged(TopologyChangedEvent<?, ?> tce, String indeName) {
      String shardId = indeName.substring(indeName.lastIndexOf('.') + 1);
      ConsistentHash consistentHashAtStart = tce.getConsistentHashAtStart();
      ConsistentHash consistentHashAtEnd = tce.getConsistentHashAtEnd();
      ShardDistribution shardDistributionBefore = this.buildShardDistribution(consistentHashAtStart);
      ShardDistribution shardDistributionAfter = this.buildShardDistribution(consistentHashAtEnd);
      return !shardDistributionBefore.getOwner(shardId).equals(shardDistributionAfter.getOwner(shardId));
   }

   @TopologyChanged
   @SuppressWarnings("unused")
   public void onTopologyChange(TopologyChangedEvent<?, ?> tce) {
      if (initialized) {
         logger.debugf("Updating shard allocation");
         this.shardDistribution = this.buildShardDistribution(distributionManager.getWriteConsistentHash());
      }
   }

}
