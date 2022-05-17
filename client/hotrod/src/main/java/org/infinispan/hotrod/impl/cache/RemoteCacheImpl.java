package org.infinispan.hotrod.impl.cache;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import javax.transaction.TransactionManager;

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
import org.infinispan.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.hotrod.filter.Filters;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.HotRodTransport;
import org.infinispan.hotrod.impl.iteration.RemotePublisher;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.hotrod.impl.operations.GetOperation;
import org.infinispan.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.hotrod.impl.operations.PingResponse;
import org.infinispan.hotrod.impl.operations.PutIfAbsentOperation;
import org.infinispan.hotrod.impl.operations.PutOperation;
import org.infinispan.hotrod.impl.operations.RemoveOperation;
import org.infinispan.hotrod.impl.operations.RetryAwareCompletionStage;
import org.infinispan.hotrod.near.NearCacheService;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

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
   public CompletionStage<V> putIfAbsent(K key, V value, CacheWriteOptions options) {
      PutIfAbsentOperation<K, V> op = cacheOperationsFactory.newPutIfAbsentOperation(keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(value), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options) {
      PutIfAbsentOperation<K, Boolean> op = cacheOperationsFactory.newPutIfAbsentOperation(keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(value), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<V> put(K key, V value, CacheWriteOptions options) {
      PutOperation<K, V> op = cacheOperationsFactory.newPutKeyValueOperation(keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(value), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<Void> set(K key, V value, CacheWriteOptions options) {
      PutOperation<K, Void> op = cacheOperationsFactory.newPutKeyValueOperation(keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(value), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheOptions options) {
      RemoveOperation<K, Boolean> op = cacheOperationsFactory.newRemoveOperation(keyAsObjectIfNeeded(key), keyToBytes(key), options, dataFormat);
      return op.execute();
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheEntryVersion version, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<V> getAndRemove(K key, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<K> keys(CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> entries(CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> putAll(Map<K, V> entries, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> putAll(Flow.Publisher<CacheEntry<K, V>> entries, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(CacheOptions options, K[] keys) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<K> removeAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Long> estimateSize(CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> clear(CacheOptions options) {
      throw new UnsupportedOperationException();
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
