package org.infinispan.client.hotrod.impl.operations;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.commons.util.IntSet;

import io.netty.channel.Channel;

public class DefaultCacheOperationsFactory implements CacheOperationsFactory {
   private final InternalRemoteCache<?, ?> remoteCache;

   public DefaultCacheOperationsFactory(InternalRemoteCache<?, ?> remoteCache) {
      this.remoteCache = Objects.requireNonNull(remoteCache);
   }

   public InternalRemoteCache<?, ?> getRemoteCache() {
      return remoteCache;
   }

   @Override
   public <V> HotRodOperation<V> newGetOperation(Object key) {
      return new GetOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key));
   }

   @Override
   public HotRodOperation<PingResponse> newPingOperation() {
      return new CachePingOperation(remoteCache.getName());
   }

   @Override
   public <T> HotRodOperation<T> executeOperation(String taskName, Map<String, byte[]> marshalledParams, Object key) {
      return new CacheExecuteOperation<>(remoteCache, taskName, marshalledParams,
            key != null ? remoteCache.getDataFormat().keyToBytes(key) : null);
   }

   @Override
   public PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit,
                                                                     List<Modification> modifications,
                                                                     boolean recoverable, long timeoutMs) {
      return new PrepareTransactionOperation(remoteCache, xid, onePhaseCommit, modifications, recoverable, timeoutMs);
   }

   @Override
   public HotRodOperation<Void> newRemoveClientListenerOperation(Object listener) {
      ClientListenerNotifier cln = remoteCache.getDispatcher().getClientListenerNotifier();
      byte[] listenerId = cln.findListenerId(listener);
      if (listenerId == null) {
         return NoHotRodOperation.instance();
      }
      return new RemoveClientListenerOperation(remoteCache, cln, listenerId);
   }

   @Override
   public HotRodOperation<IterationStartResponse> newIterationStartOperation(String filterConverterFactory, byte[][] filterParams,
                                                                             IntSet segments, int batchSize, boolean metadata) {
      return new IterationStartOperation(remoteCache, filterConverterFactory, filterParams, segments, batchSize,
            metadata);
   }

   @Override
   public <K, E> HotRodOperation<IterationNextResponse<K, E>> newIterationNextOperation(byte[] iterationId, KeyTracker segmentKeyTracker) {
      return new IterationNextOperation<>(remoteCache, iterationId, segmentKeyTracker);
   }

   @Override
   public HotRodOperation<IterationEndResponse> newIterationEndOperation(byte[] iterationId) {
      return new IterationEndOperation(remoteCache, iterationId);
   }

   @Override
   public ClearOperation newClearOperation() {
      return new ClearOperation(remoteCache);
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      // TODO: need to support when storage is object based
      return new PutOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <V> HotRodOperation<MetadataValue<V>> newRemoveOperation(Object key) {
      return new RemoveOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key));
   }

   @Override
   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key) {
      return new ContainsKeyOperation(remoteCache, remoteCache.getDataFormat().keyToBytes(key));
   }

   @Override
   public <K, V> HotRodOperation<V> newReplaceOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                        long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new ReplaceOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                            long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new PutIfAbsentOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                            long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return new PutIfAbsentOperation<>(remoteCache.withFlags(flags), remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public HotRodOperation<ServerStatistics> newStatsOperation() {
      return new StatsOperation(remoteCache);
   }

   @Override
   public HotRodOperation<Integer> newSizeOperation() {
      return new SizeOperation(remoteCache);
   }

   @Override
   public <K, V> HotRodOperation<GetWithMetadataOperation.GetWithMetadataResult<V>> newGetWithMetadataOperation(K key, Channel channel) {
      return new GetWithMetadataOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key), channel);
   }

   @Override
   public <K, V> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan,
                                                                                                TimeUnit lifespanTimeUnit, long maxIdle,
                                                                                                TimeUnit maxIdleTimeUnit, long version) {
      return new ReplaceIfUnmodifiedOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(value), lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version);
   }

   @Override
   public <K, V> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version) {
      return new RemoveIfUnmodifiedOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key), version);
   }

   @Override
   public PutAllOperation newPutAllBytesOperation(Map<byte[], byte[]> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new PutAllOperation(remoteCache, map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <K, V> GetAllOperation<K, V> newGetAllBytesOperation(Set<byte[]> keys) {
      return new GetAllOperation<>(remoteCache, keys);
   }

   @Override
   public HotRodOperation<Void> newUpdateBloomFilterOperation(byte[] bloomFilterBits) {
      return new UpdateBloomFilterOperation(remoteCache, bloomFilterBits);
   }

   @Override
   public ClientListenerOperation newAddNearCacheListenerOperation(Object listener, int bloomBits) {
      return new AddBloomNearCacheClientListenerOperation(remoteCache, listener, bloomBits);
   }

   @Override
   public <T> QueryOperation<T> newQueryOperation(RemoteQuery<T> ts, boolean withHitCount) {
      return new QueryOperation<>(remoteCache, ts, withHitCount);
   }

   @Override
   public AddClientListenerOperation newAddClientListenerOperation(Object listener) {
      return newAddClientListenerOperation(listener, null, null);
   }

   @Override
   public AddClientListenerOperation newAddClientListenerOperation(Object listener, Object[] filterFactoryParams,
                                                                   Object[] converterFactoryParams) {
      return new AddClientListenerOperation(remoteCache, listener, marshallParams(filterFactoryParams),
            marshallParams(converterFactoryParams));
   }

   @Override
   public byte[][] marshallParams(Object[] params) {
      if (params == null)
         return org.infinispan.commons.util.Util.EMPTY_BYTE_ARRAY_ARRAY;

      byte[][] marshalledParams = new byte[params.length][];
      for (int i = 0; i < marshalledParams.length; i++) {
         byte[] bytes = remoteCache.getDataFormat().keyToBytes(params[i]);// should be small
         marshalledParams[i] = bytes;
      }

      return marshalledParams;
   }

   @Override
   public CacheOperationsFactory newFactoryFor(InternalRemoteCache<?, ?> internalRemoteCache) {
      return new DefaultCacheOperationsFactory(internalRemoteCache);
   }
}
