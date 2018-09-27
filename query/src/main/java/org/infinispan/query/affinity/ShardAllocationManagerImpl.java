package org.infinispan.query.affinity;

import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
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

   private static final Log log = LogFactory.getLog(ShardAllocationManagerImpl.class, Log.class);

   private final DistributionManager distributionManager;

   /**
    * Indicates if {@link #numSegments} and {@link #numShards} are initialized.
    */
   private volatile boolean initialized;
   private int numSegments;
   private int numShards;

   private volatile ShardDistribution shardDistribution;

   public ShardAllocationManagerImpl(DistributionManager distributionManager, Cache<?, ?> cache) {
      this.distributionManager = distributionManager;
      cache.addListener(this);
   }

   @Override
   public String getShardFromSegment(int segment) {
      String shardId = getShardDistribution().getShardFromSegment(segment);
      log.debugf("ShardId for segment %d: %s", segment, shardId);
      return shardId;
   }

   private ShardDistribution buildShardDistribution(ConsistentHash consistentHash) {
      if (!initialized) {
         throw new IllegalStateException("Not initialized yet!");
      }
      if (consistentHash == null) {
         return new LocalModeShardDistribution(numShards);
      }
      return numShards == numSegments ? new PerSegmentShardDistribution(consistentHash) :
            new FixedShardsDistribution(consistentHash, numShards);
   }

   private ShardDistribution getShardDistribution() {
      if (shardDistribution == null) {
         shardDistribution = buildShardDistribution(
               distributionManager == null ? null : distributionManager.getWriteConsistentHash());
      }
      return shardDistribution;
   }

   @Override
   public Address getOwner(String shardId) {
      return getShardDistribution().getOwner(shardId);
   }

   @Override
   public String getShardFromKey(Object key) {
      int segment = distributionManager != null ? distributionManager.getCacheTopology().getSegment(key) : 0;
      log.debugf("Segment for key %s: %d", key, segment);
      return getShardDistribution().getShardFromSegment(segment);
   }

   @Override
   public void initialize(int numShards, int numSegments) {
      this.numSegments = numSegments;
      this.numShards = numShards;
      initialized = true;
   }

   @Override
   public Set<String> getShards() {
      Set<String> shards = getShardDistribution().getShardsIdentifiers();
      log.debugf("AllShards:%s", shards);
      return shards;
   }

   @Override
   public Set<String> getShardsForModification(Address address) {
      Set<String> shards = getShardDistribution().getShards(address);
      log.debugf("Shard for modification %s for address %s", shards, address);
      return shards;
   }

   @Override
   public boolean isOwnershipChanged(TopologyChangedEvent<?, ?> tce, String indexName) {
      String shardId = indexName.substring(indexName.lastIndexOf('.') + 1);
      ConsistentHash consistentHashAtStart = tce.getConsistentHashAtStart();
      ConsistentHash consistentHashAtEnd = tce.getConsistentHashAtEnd();
      ShardDistribution shardDistributionBefore = buildShardDistribution(consistentHashAtStart);
      ShardDistribution shardDistributionAfter = buildShardDistribution(consistentHashAtEnd);
      return !shardDistributionBefore.getOwner(shardId).equals(shardDistributionAfter.getOwner(shardId));
   }

   @TopologyChanged
   @SuppressWarnings("unused")
   public void onTopologyChange(TopologyChangedEvent<?, ?> tce) {
      if (initialized) {
         log.debug("Updating shard allocation due to topology change");
         shardDistribution = buildShardDistribution(distributionManager.getWriteConsistentHash());
      }
   }
}
