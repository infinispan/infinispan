package org.infinispan.hotrod.impl.cache;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import jakarta.transaction.TransactionManager;

import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.hotrod.filter.Filters;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.HotRodTransport;
import org.infinispan.hotrod.impl.iteration.RemotePublisher;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.hotrod.impl.operations.ClearOperation;
import org.infinispan.hotrod.impl.operations.GetAllParallelOperation;
import org.infinispan.hotrod.impl.operations.GetAndRemoveOperation;
import org.infinispan.hotrod.impl.operations.GetOperation;
import org.infinispan.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.hotrod.impl.operations.PingResponse;
import org.infinispan.hotrod.impl.operations.PutAllParallelOperation;
import org.infinispan.hotrod.impl.operations.PutIfAbsentOperation;
import org.infinispan.hotrod.impl.operations.PutOperation;
import org.infinispan.hotrod.impl.operations.RemoveIfUnmodifiedOperation;
import org.infinispan.hotrod.impl.operations.RemoveOperation;
import org.infinispan.hotrod.impl.operations.ReplaceIfUnmodifiedOperation;
import org.infinispan.hotrod.impl.operations.RetryAwareCompletionStage;
import org.infinispan.hotrod.impl.operations.SetIfAbsentOperation;
import org.infinispan.hotrod.impl.operations.SetOperation;
import org.infinispan.hotrod.near.NearCacheService;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> implements RemoteCache<K, V> {
   private static final Log log = LogFactory.getLog(RemoteCacheImpl.class, Log.class);

   private final String name;
   private final HotRodTransport hotRodTransport;
   private final CacheOperationsFactory cacheOperationsFactory;
   private volatile boolean isObjectStorage;
   private final DataFormat dataFormat;
   private final ClientStatistics clientStatistics;

   public RemoteCacheImpl(HotRodTransport hotRodTransport, String name, TimeService timeService, NearCacheService<K, V> nearCacheService) {
      this(hotRodTransport, name, new ClientStatistics(hotRodTransport.getConfiguration().statistics().enabled(), timeService, nearCacheService), DataFormat.builder().build());
      hotRodTransport.getMBeanHelper().register(this);
   }

   private RemoteCacheImpl(RemoteCacheImpl<?, ?> instance, DataFormat dataFormat) {
      this(instance.hotRodTransport, instance.name, instance.clientStatistics, dataFormat);
   }

   private RemoteCacheImpl(HotRodTransport hotRodTransport, String name, ClientStatistics clientStatistics, DataFormat dataFormat) {
      this.name = name;
      this.hotRodTransport = hotRodTransport;
      this.dataFormat = dataFormat;
      this.clientStatistics = clientStatistics;
      this.cacheOperationsFactory = hotRodTransport.createCacheOperationFactory(name, clientStatistics);
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public DataFormat getDataFormat() {
      return dataFormat;
   }

   @Override
   public CompletionStage<CacheConfiguration> configuration() {
      throw new UnsupportedOperationException();
   }

   @Override
   public HotRodTransport getHotRodTransport() {
      return hotRodTransport;
   }

   @Override
   public CacheOperationsFactory getOperationsFactory() {
      return cacheOperationsFactory;
   }

   @Override
   public CompletionStage<V> get(K key, CacheOptions options) {
      byte[] keyBytes = keyToBytes(key);
      GetOperation<K, V> gco = cacheOperationsFactory.newGetKeyOperation(keyAsObjectIfNeeded(key), keyBytes, options, dataFormat);
      CompletionStage<V> result = gco.execute();
      if (log.isTraceEnabled()) {
         result.thenAccept(value -> log.tracef("For key(%s) returning %s", key, value));
      }
      return result;
   }

   @Override
   public K keyAsObjectIfNeeded(Object key) {
      return isObjectStorage ? (K) key : null;
   }

   @Override
   public void close() {
      hotRodTransport.getMBeanHelper().unregister(this);
   }

   @Override
   public SocketAddress addNearCacheListener(Object listener, int bloomBits) {
      throw new UnsupportedOperationException("Adding a near cache listener to a RemoteCache is not supported!");
   }

   private byte[][] marshallParams(Object[] params) {
      if (params == null) return org.infinispan.commons.util.Util.EMPTY_BYTE_ARRAY_ARRAY;

      byte[][] marshalledParams = new byte[params.length][];
      for (int i = 0; i < marshalledParams.length; i++) {
         byte[] bytes = keyToBytes(params[i]);// should be small
         marshalledParams[i] = bytes;
      }

      return marshalledParams;
   }

   public CompletionStage<PingResponse> ping() {
      return cacheOperationsFactory.newFaultTolerantPingOperation().execute();
   }

   @Override
   public byte[] keyToBytes(Object o) {
      return dataFormat.keyToBytes(o);
   }

   @Override
   public byte[] valueToBytes(Object o) {
      return dataFormat.valueToBytes(o);
   }

   @Override
   public RetryAwareCompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options) {
      GetWithMetadataOperation<K, V> op = cacheOperationsFactory.newGetWithMetadataOperation(keyAsObjectIfNeeded(key), keyToBytes(key), options, dataFormat);
      return op.internalExecute();
   }

   @Override
   public RetryAwareCompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options, SocketAddress listenerAddress) {
      GetWithMetadataOperation<K, V> op = cacheOperationsFactory.newGetWithMetadataOperation(keyAsObjectIfNeeded(key), keyToBytes(key), options, dataFormat, listenerAddress);
      return op.internalExecute();
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> putIfAbsent(K key, V value, CacheWriteOptions options) {
      PutIfAbsentOperation<K, V> op = cacheOperationsFactory.newPutIfAbsentOperation(keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(value), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options) {
      SetIfAbsentOperation<K> op = cacheOperationsFactory.newSetIfAbsentOperation(keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(value), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> put(K key, V value, CacheWriteOptions options) {
      PutOperation<K, V> op = cacheOperationsFactory.newPutKeyValueOperation(keyAsObjectIfNeeded(key), keyToBytes(key),
            valueToBytes(value), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<Void> set(K key, V value, CacheWriteOptions options) {
      SetOperation<K> op = cacheOperationsFactory.newSetKeyValueOperation(keyAsObjectIfNeeded(key), keyToBytes(key),
            valueToBytes(value), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getAndRemove(K key, CacheOptions options) {
      GetAndRemoveOperation<K, V> op = cacheOperationsFactory.newGetAndRemoveOperation(keyAsObjectIfNeeded(key),
            keyToBytes(key), options, dataFormat);
      return op.execute();
   }


   @Override
   public CompletionStage<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      if (!(Objects.requireNonNull(version) instanceof CacheEntryVersionImpl)) {
         throw new IllegalArgumentException("Only CacheEntryVersionImpl instances are supported!");
      }
      ReplaceIfUnmodifiedOperation<K, V> op = cacheOperationsFactory.newReplaceIfUnmodifiedOperation(keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(value),
            ((CacheEntryVersionImpl) version).version(), options, dataFormat);
      // TODO: add new op to prevent requiring return value?
      return op.execute().thenApply(r -> r.getCode().isUpdated());
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      if (!(Objects.requireNonNull(version) instanceof CacheEntryVersionImpl)) {
         throw new IllegalArgumentException("Only CacheEntryVersionImpl instances are supported!");
      }
      ReplaceIfUnmodifiedOperation<K, V> op = cacheOperationsFactory.newReplaceIfUnmodifiedOperation(keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(value),
            ((CacheEntryVersionImpl) version).version(), options, dataFormat);
      return op.execute().thenApply(r -> {
         if (r.getCode().isUpdated()) {
            return null;
         }
         return r.getValue();
      });
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheOptions options) {
      RemoveOperation<K> op = cacheOperationsFactory.newRemoveOperation(keyAsObjectIfNeeded(key), keyToBytes(key), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheEntryVersion version, CacheOptions options) {
      if (!(Objects.requireNonNull(version) instanceof CacheEntryVersionImpl)) {
         throw new IllegalArgumentException("Only CacheEntryVersionImpl instances are supported!");
      }
      RemoveIfUnmodifiedOperation<K, V> op = cacheOperationsFactory.newRemoveIfUnmodifiedOperation(key, keyToBytes(key),
            ((CacheEntryVersionImpl) version).version(), options, dataFormat);
      // TODO: add new op to prevent requiring return value?
      return op.execute().thenApply(r -> r.getValue() != null);
   }

   @Override
   public Flow.Publisher<K> keys(CacheOptions options) {
      assertRemoteCacheManagerIsStarted();
      Flowable<K> flowable = Flowable.fromPublisher(new RemotePublisher<K, Object>(cacheOperationsFactory,
                  "org.infinispan.server.hotrod.HotRodServer$ToEmptyBytesKeyValueFilterConverter",
                  Util.EMPTY_BYTE_ARRAY_ARRAY, null, 128, false, dataFormat))
            .map(CacheEntry::key);
      return FlowAdapters.toFlowPublisher(flowable);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> entries(CacheOptions options) {
      assertRemoteCacheManagerIsStarted();
      return FlowAdapters.toFlowPublisher(new RemotePublisher<>(cacheOperationsFactory, null,
            Util.EMPTY_BYTE_ARRAY_ARRAY, null, 128, false, dataFormat));

   }

   @Override
   public CompletionStage<Void> putAll(Map<K, V> entries, CacheWriteOptions options) {
      PutAllParallelOperation putAllParallelOperation = cacheOperationsFactory.newPutAllOperation(
            entries.entrySet().stream().collect(Collectors.toMap(node -> keyToBytes(node.getKey()), node -> valueToBytes(node.getValue()))), options, dataFormat);
      return putAllParallelOperation.execute();
   }

   @Override
   public CompletionStage<Void> putAll(Flow.Publisher<CacheEntry<K, V>> entries, CacheWriteOptions options) {
      // TODO: this ignores the metadata, however expiration is part of options... is that okay?
      return Flowable.fromPublisher(FlowAdapters.toPublisher(entries))
            .collect(Collectors.toMap(CacheEntry::key, CacheEntry::value))
            .concatMapCompletable(map -> Completable.fromCompletionStage(putAll(map, options)))
            .toCompletionStage(null);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options) {
      GetAllParallelOperation<K, V> op = cacheOperationsFactory.newGetAllOperation(keys.stream().map(this::keyToBytes).collect(Collectors.toSet()), options, dataFormat);
      Flowable<CacheEntry<K, V>> flowable = Flowable.defer(() -> Flowable.fromCompletionStage(op.execute())
            .concatMapIterable(Map::entrySet)
            // TODO: need to worry about metadata
            .map(e -> new CacheEntryImpl<>(e.getKey(), e.getValue(), null)));
      return FlowAdapters.toFlowPublisher(flowable);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(CacheOptions options, K[] keys) {
      GetAllParallelOperation<K, V> op = cacheOperationsFactory.newGetAllOperation(Arrays.stream(keys).map(this::keyToBytes).collect(Collectors.toSet()), options, dataFormat);
      Flowable<CacheEntry<K, V>> flowable = Flowable.defer(() -> Flowable.fromCompletionStage(op.execute())
            .concatMapIterable(Map::entrySet)
            // TODO: need to worry about metadata
            .map(e -> new CacheEntryImpl<>(e.getKey(), e.getValue(), null)));
      return FlowAdapters.toFlowPublisher(flowable);
   }

   private Flow.Publisher<K> removeAll(Flowable<K> keys, CacheWriteOptions options) {
      Flowable<K> keyFlowable = keys.concatMapMaybe(k -> Single.fromCompletionStage(remove(k, options)).mapOptional(removed ->
            removed ? Optional.of(k) : Optional.empty()));
      return FlowAdapters.toFlowPublisher(keyFlowable);
   }

   @Override
   public Flow.Publisher<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      return removeAll(Flowable.fromIterable(keys), options);
   }

   @Override
   public Flow.Publisher<K> removeAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return removeAll(Flowable.fromPublisher(FlowAdapters.toPublisher(keys)), options);
   }

   private Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flowable<K> keys, CacheWriteOptions options) {
      // TODO: eventually support a batch getAndRemoveAll by owner
      Flowable<CacheEntry<K, V>> entryFlowable = keys
            .concatMapMaybe(k -> Maybe.fromCompletionStage(getAndRemove(k, options)));
      return FlowAdapters.toFlowPublisher(entryFlowable);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options) {
      return getAndRemoveAll(Flowable.fromIterable(keys), options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return getAndRemoveAll(Flowable.fromPublisher(FlowAdapters.toPublisher(keys)), options);
   }

   @Override
   public CompletionStage<Long> estimateSize(CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> clear(CacheOptions options) {
      // Currently, no options are used by clear (what about timeout?)
      ClearOperation op = cacheOperationsFactory.newClearOperation();
      return op.execute();
   }

   @Override
   public Flow.Publisher<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType[] types) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Set<K> keys, AsyncCacheEntryProcessor<K, V, T> task, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> processAll(AsyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      throw new UnsupportedOperationException();
   }

   protected void assertRemoteCacheManagerIsStarted() {
      if (!hotRodTransport.isStarted()) {
         String message = "Cannot perform operations on a cache associated with an unstarted RemoteCacheManager. Use RemoteCacheManager.start before using the remote cache.";
         HOTROD.unstartedRemoteCacheManager();
         throw new RemoteCacheManagerNotStartedException(message);
      }
   }

   @Override
   public <K1, V1> RemoteCache<K1, V1> withDataFormat(DataFormat newDataFormat) {
      Objects.requireNonNull(newDataFormat, "Data Format must not be null").initialize(hotRodTransport, name, isObjectStorage);
      return new RemoteCacheImpl<K1, V1>(this, newDataFormat);
   }

   @Override
   public void resolveStorage(boolean objectStorage) {
      this.isObjectStorage = objectStorage;
      this.dataFormat.initialize(hotRodTransport, name, isObjectStorage);
   }

   public boolean isObjectStorage() {
      return isObjectStorage;
   }

   @Override
   public CompletionStage<Void> updateBloomFilter() {
      return CompletableFuture.completedFuture(null);
   }

   @Override
   public String toString() {
      return "RemoteCacheImpl " + name;
   }

   public ClientStatistics getClientStatistics() {
      return clientStatistics;
   }

   @Override
   public CloseableIterator<CacheEntry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      Publisher<CacheEntry<K, Object>> remotePublisher = publishEntries(filterConverterFactory, filterConverterParams, segments, batchSize);
      //noinspection unchecked
      return Closeables.iterator((Publisher) remotePublisher, batchSize);
   }

   @Override
   public <E> Publisher<CacheEntry<K, E>> publishEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      assertRemoteCacheManagerIsStarted();
      if (segments != null && segments.isEmpty()) {
         return Flowable.empty();
      }
      byte[][] params = marshallParams(filterConverterParams);
      return new RemotePublisher<>(cacheOperationsFactory, filterConverterFactory, params, segments,
            batchSize, false, dataFormat);
   }

   @Override
   public CloseableIterator<CacheEntry<Object, Object>> retrieveEntriesByQuery(RemoteQuery query, Set<Integer> segments, int batchSize) {
      Publisher<CacheEntry<K, Object>> remotePublisher = publishEntriesByQuery(query, segments, batchSize);
      //noinspection unchecked
      return Closeables.iterator((Publisher) remotePublisher, batchSize);
   }

   @Override
   public <E> Publisher<CacheEntry<K, E>> publishEntriesByQuery(RemoteQuery query, Set<Integer> segments, int batchSize) {
      return publishEntries(Filters.ITERATION_QUERY_FILTER_CONVERTER_FACTORY_NAME, query.toFactoryParams(), segments, batchSize);
   }

   @Override
   public CloseableIterator<CacheEntry<Object, Object>> retrieveEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      Publisher<CacheEntry<K, V>> remotePublisher = publishEntriesWithMetadata(segments, batchSize);
      //noinspection unchecked
      return Closeables.iterator((Publisher) remotePublisher, batchSize);
   }

   @Override
   public Publisher<CacheEntry<K, V>> publishEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      return new RemotePublisher<>(cacheOperationsFactory, null, null, segments,
            batchSize, true, dataFormat);
   }

   @Override
   public TransactionManager getTransactionManager() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isTransactional() {
      return false;
   }
}
