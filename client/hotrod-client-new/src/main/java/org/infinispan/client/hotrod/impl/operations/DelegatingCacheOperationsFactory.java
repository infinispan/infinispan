package org.infinispan.client.hotrod.impl.operations;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.commons.util.IntSet;

import io.netty.channel.Channel;

public abstract class DelegatingCacheOperationsFactory implements CacheOperationsFactory {
   private final CacheOperationsFactory delegate;

   public DelegatingCacheOperationsFactory(CacheOperationsFactory delegate) {
      this.delegate = delegate;
   }

   @Override
   public InternalRemoteCache<?, ?> getRemoteCache() {
      return delegate.getRemoteCache();
   }

   @Override
   public <V> HotRodOperation<V> newGetOperation(Object key) {
      return delegate.newGetOperation(key);
   }

   @Override
   public HotRodOperation<PingResponse> newPingOperation() {
      return delegate.newPingOperation();
   }

   @Override
   public <T> HotRodOperation<T> executeOperation(String taskName, Map<String, byte[]> marshalledParams, Object key) {
      return delegate.executeOperation(taskName, marshalledParams, key);
   }

   @Override
   public PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit, List<Modification> modifications, boolean recoverable, long timeoutMs) {
      return delegate.newPrepareTransactionOperation(xid, onePhaseCommit, modifications, recoverable, timeoutMs);
   }

   @Override
   public HotRodOperation<Void> newRemoveClientListenerOperation(Object listener) {
      return delegate.newRemoveClientListenerOperation(listener);
   }

   @Override
   public HotRodOperation<IterationStartResponse> newIterationStartOperation(String filterConverterFactory, byte[][] filterParams, IntSet segments, int batchSize, boolean metadata) {
      return delegate.newIterationStartOperation(filterConverterFactory, filterParams, segments, batchSize, metadata);
   }

   @Override
   public <K, E> HotRodOperation<IterationNextResponse<K, E>> newIterationNextOperation(byte[] iterationId, KeyTracker segmentKeyTracker) {
      return delegate.newIterationNextOperation(iterationId, segmentKeyTracker);
   }

   @Override
   public HotRodOperation<IterationEndResponse> newIterationEndOperation(byte[] iterationId) {
      return delegate.newIterationEndOperation(iterationId);
   }

   @Override
   public HotRodOperation<Void> newClearOperation() {
      return delegate.newClearOperation();
   }

   @Override
   public <V, K> HotRodOperation<V> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.newPutKeyValueOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <V> HotRodOperation<V> newRemoveOperation(Object key) {
      return delegate.newRemoveOperation(key);
   }

   @Override
   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key) {
      return delegate.newContainsKeyOperation(key);
   }

   @Override
   public <V, K> HotRodOperation<V> newReplaceOperation(K key, V valueBytes, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.newReplaceOperation(key, valueBytes, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return delegate.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   @Override
   public HotRodOperation<ServerStatistics> newStatsOperation() {
      return delegate.newStatsOperation();
   }

   @Override
   public HotRodOperation<Integer> newSizeOperation() {
      return delegate.newSizeOperation();
   }

   @Override
   public HotRodOperation<Void> newPutAllBytesOperation(Map<byte[], byte[]> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.newPutAllBytesOperation(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <V, K> HotRodOperation<GetWithMetadataOperation.GetWithMetadataResult<V>> newGetWithMetadataOperation(K key, Channel preferredChannel) {
      return delegate.newGetWithMetadataOperation(key, preferredChannel);
   }

   @Override
   public <V, K> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version) {
      return delegate.newReplaceIfUnmodifiedOperation(key, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version);
   }

   @Override
   public <V, K> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version) {
      return delegate.newRemoveIfUnmodifiedOperation(key, version);
   }

   @Override
   public <K, V> HotRodOperation<Map<K, V>> newGetAllBytesOperation(Set<byte[]> keys) {
      return delegate.newGetAllBytesOperation(keys);
   }

   @Override
   public HotRodOperation<Void> newUpdateBloomFilterOperation(byte[] bloomFilterBits) {
      return delegate.newUpdateBloomFilterOperation(bloomFilterBits);
   }

   @Override
   public ClientListenerOperation newAddNearCacheListenerOperation(Object listener, int bloomBits) {
      return delegate.newAddNearCacheListenerOperation(listener, bloomBits);
   }

   @Override
   public <T> QueryOperation<T> newQueryOperation(RemoteQuery<T> ts, boolean withHitCount) {
      return delegate.newQueryOperation(ts, withHitCount);
   }

   @Override
   public AddClientListenerOperation newAddClientListenerOperation(Object listener) {
      return delegate.newAddClientListenerOperation(listener);
   }

   @Override
   public AddClientListenerOperation newAddClientListenerOperation(Object listener, Object[] filterFactoryParams, Object[] converterFactoryParams) {
      return delegate.newAddClientListenerOperation(listener, filterFactoryParams, converterFactoryParams);
   }

   @Override
   public byte[][] marshallParams(Object[] params) {
      return delegate.marshallParams(params);
   }

   protected abstract DelegatingCacheOperationsFactory newFactoryFor(CacheOperationsFactory factory);

   @Override
   public final CacheOperationsFactory newFactoryFor(InternalRemoteCache<?, ?> internalRemoteCache) {
      return newFactoryFor(delegate.newFactoryFor(internalRemoteCache));
   }
}
