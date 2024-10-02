package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;

import io.netty.channel.Channel;

public class StatsOperationsFactory extends DelegatingCacheOperationsFactory {
   private final ClientStatistics statistics;
   public StatsOperationsFactory(CacheOperationsFactory delegate, ClientStatistics statistics) {
      super(delegate);
      this.statistics = statistics;
   }

   @Override
   public <V> HotRodOperation<V> newGetOperation(Object key) {
      return new StatisticOperation<>(super.newGetOperation(key), statistics);
   }

   @Override
   public <K, V> HotRodOperation<GetWithMetadataOperation.GetWithMetadataResult<V>> newGetWithMetadataOperation(K key, Channel preferredChannel) {
      return new StatisticOperation<>(super.newGetWithMetadataOperation(key, preferredChannel), statistics);
   }

   @Override
   public <V> HotRodOperation<MetadataValue<V>> newRemoveOperation(Object key) {
      return new StatisticOperation<>(super.newRemoveOperation(key), statistics);
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new StatisticOperation<>(super.newPutKeyValueOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), statistics);
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new StatisticOperation<>(super.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), statistics);
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return new StatisticOperation<>(super.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags), statistics);
   }

   @Override
   public <K, V> HotRodOperation<V> newReplaceOperation(K key, V valueBytes, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new StatisticOperation<>(super.newReplaceOperation(key, valueBytes, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), statistics);
   }

   @Override
   public <K, V> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version) {
      return new StatisticOperation<>(super.newReplaceIfUnmodifiedOperation(key, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version), statistics);
   }

   @Override
   public <K, V> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version) {
      return new StatisticOperation<>(super.newRemoveIfUnmodifiedOperation(key, version), statistics);
   }

   @Override
   public HotRodOperation<Void> newPutAllBytesOperation(Map<byte[], byte[]> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new StatisticOperation<>(super.newPutAllBytesOperation(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), statistics);
   }

   @Override
   public <K, V> HotRodOperation<Map<K, V>> newGetAllBytesOperation(Set<byte[]> keys) {
      return new StatisticOperation<>(super.newGetAllBytesOperation(keys), statistics);
   }

   //   @Override
   //   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key) {
//      return new RoutingObjectOperation<>(super.newContainsKeyOperation(key), key);
//   }

   @Override
   protected DelegatingCacheOperationsFactory newFactoryFor(CacheOperationsFactory factory) {
      return new StatsOperationsFactory(factory, statistics);
   }
}
