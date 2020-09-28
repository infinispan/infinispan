package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.filter.Filters.makeFactoryParams;
import static org.infinispan.client.hotrod.impl.Util.await;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.StreamingRemoteCache;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.StatisticsConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.client.hotrod.impl.iteration.RemotePublisher;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.ClearOperation;
import org.infinispan.client.hotrod.impl.operations.ContainsKeyOperation;
import org.infinispan.client.hotrod.impl.operations.ExecuteOperation;
import org.infinispan.client.hotrod.impl.operations.GetAllParallelOperation;
import org.infinispan.client.hotrod.impl.operations.GetOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingResponse;
import org.infinispan.client.hotrod.impl.operations.PutAllParallelOperation;
import org.infinispan.client.hotrod.impl.operations.PutIfAbsentOperation;
import org.infinispan.client.hotrod.impl.operations.PutOperation;
import org.infinispan.client.hotrod.impl.operations.RemoveClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.RemoveIfUnmodifiedOperation;
import org.infinispan.client.hotrod.impl.operations.RemoveOperation;
import org.infinispan.client.hotrod.impl.operations.ReplaceIfUnmodifiedOperation;
import org.infinispan.client.hotrod.impl.operations.ReplaceOperation;
import org.infinispan.client.hotrod.impl.operations.RetryAwareCompletionStage;
import org.infinispan.client.hotrod.impl.operations.SizeOperation;
import org.infinispan.client.hotrod.impl.operations.StatsOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.query.dsl.Query;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> extends RemoteCacheSupport<K, V> implements InternalRemoteCache<K, V> {

   private static final Log log = LogFactory.getLog(RemoteCacheImpl.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private Marshaller defaultMarshaller;
   private final String name;
   private final RemoteCacheManager remoteCacheManager;
   protected OperationsFactory operationsFactory;
   private int batchSize;
   private volatile boolean isObjectStorage;
   private final DataFormat defaultDataFormat;
   private DataFormat dataFormat;
   protected ClientStatistics clientStatistics;
   private ObjectName mbeanObjectName;

   public RemoteCacheImpl(RemoteCacheManager rcm, String name, TimeService timeService) {
      this(rcm, name, timeService, null);
   }

   public RemoteCacheImpl(RemoteCacheManager rcm, String name, TimeService timeService, NearCacheService<K, V> nearCacheService) {
      if (trace) {
         log.tracef("Creating remote cache: %s", name);
      }
      this.name = name;
      this.remoteCacheManager = rcm;
      this.defaultDataFormat = DataFormat.builder().build();
      this.clientStatistics = new ClientStatistics(rcm.getConfiguration().statistics().enabled(), timeService, nearCacheService);
   }

   protected RemoteCacheImpl(RemoteCacheManager rcm, String name, ClientStatistics clientStatistics) {
      if (trace) {
         log.tracef("Creating remote cache: %s", name);
      }
      this.name = name;
      this.remoteCacheManager = rcm;
      this.defaultDataFormat = DataFormat.builder().build();
      this.clientStatistics = clientStatistics;
   }

   @Override
   public void init(Marshaller marshaller, OperationsFactory operationsFactory,
                    Configuration configuration, ObjectName jmxParent) {
      init(marshaller, operationsFactory, configuration);
      registerMBean(jmxParent);
   }

   /**
    * Inititalize without mbeans
    */
   @Override
   public void init(Marshaller marshaller, OperationsFactory operationsFactory, Configuration configuration) {
      init(marshaller, operationsFactory,
           configuration.batchSize());
   }

   private void init(Marshaller marshaller, OperationsFactory operationsFactory,
                     int batchSize) {
      this.defaultMarshaller = marshaller;
      this.operationsFactory = operationsFactory;
      this.batchSize = batchSize;
      this.dataFormat = defaultDataFormat;
   }

   private void registerMBean(ObjectName jmxParent) {
      StatisticsConfiguration configuration = getRemoteCacheManager().getConfiguration().statistics();
      if (configuration.jmxEnabled()) {
         try {
            MBeanServer mbeanServer = configuration.mbeanServerLookup().getMBeanServer();
            String cacheName = name.isEmpty() ? "org.infinispan.default" : name;
            mbeanObjectName = new ObjectName(String.format("%s:type=HotRodClient,name=%s,cache=%s", jmxParent.getDomain(), configuration.jmxName(), cacheName));
            mbeanServer.registerMBean(clientStatistics, mbeanObjectName);
         } catch (Exception e) {
            throw HOTROD.jmxRegistrationFailure(e);
         }
      }
   }

   private void unregisterMBean() {
      if (mbeanObjectName != null) {
         try {
            MBeanServer mBeanServer = getRemoteCacheManager().getConfiguration().statistics()
                  .mbeanServerLookup().getMBeanServer();
            if (mBeanServer.isRegistered(mbeanObjectName)) {
               mBeanServer.unregisterMBean(mbeanObjectName);
            } else {
               HOTROD.debugf("MBean not registered: %s", mbeanObjectName);
            }
         } catch (Exception e) {
            throw HOTROD.jmxUnregistrationFailure(e);
         }
      }
   }

   @Override
   public OperationsFactory getOperationsFactory() {
      return operationsFactory;
   }

   @Override
   public RemoteCacheManager getRemoteCacheManager() {
      return remoteCacheManager;
   }

   @Override
   public CompletableFuture<Boolean> removeWithVersionAsync(final K key, final long version) {
      assertRemoteCacheManagerIsStarted();
      RemoveIfUnmodifiedOperation<V> op = operationsFactory.newRemoveIfUnmodifiedOperation(
            keyAsObjectIfNeeded(key), keyToBytes(key), version, dataFormat);
      return op.execute().thenApply(response -> response.getCode().isUpdated());
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      throw new UnsupportedOperationException();
   }

   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      ReplaceIfUnmodifiedOperation op = operationsFactory.newReplaceIfUnmodifiedOperation(
            keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(newValue), lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version, dataFormat);
      return op.execute().thenApply(response -> response.getCode().isUpdated());
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      Publisher<Entry<K, Object>> remotePublisher = publishEntries(filterConverterFactory, filterConverterParams, segments, batchSize);
      //noinspection unchecked
      return Closeables.iterator((Publisher) remotePublisher, batchSize);
   }

   @Override
   public <E> Publisher<Entry<K, E>> publishEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      assertRemoteCacheManagerIsStarted();
      if (segments != null && segments.isEmpty()) {
         return Flowable.empty();
      }
      byte[][] params = marshallParams(filterConverterParams);
      return new RemotePublisher<>(operationsFactory, defaultMarshaller, filterConverterFactory, params, segments,
            batchSize, false, dataFormat);
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntriesByQuery(Query<?> filterQuery, Set<Integer> segments, int batchSize) {
      Publisher<Entry<K, Object>> remotePublisher = publishEntriesByQuery(filterQuery, segments, batchSize);
      //noinspection unchecked
      return Closeables.iterator((Publisher) remotePublisher, batchSize);
   }

   @Override
   public <E> Publisher<Entry<K, E>> publishEntriesByQuery(Query<?> filterQuery, Set<Integer> segments, int batchSize) {
      Object[] factoryParams = makeFactoryParams(filterQuery);
      return publishEntries(Filters.ITERATION_QUERY_FILTER_CONVERTER_FACTORY_NAME, factoryParams, segments, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, MetadataValue<Object>>> retrieveEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      Publisher<Entry<K, MetadataValue<V>>> remotePublisher = publishEntriesWithMetadata(segments, batchSize);
      //noinspection unchecked
      return Closeables.iterator((Publisher) remotePublisher, batchSize);
   }

   @Override
   public Publisher<Entry<K, MetadataValue<V>>> publishEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      return new RemotePublisher<>(operationsFactory, defaultMarshaller, null, null, segments,
            batchSize, true, dataFormat);
   }

   @Override
   public CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key) {
      assertRemoteCacheManagerIsStarted();
      GetWithMetadataOperation<V> op = operationsFactory.newGetWithMetadataOperation(
            keyAsObjectIfNeeded(key), keyToBytes(key), dataFormat);
      return op.execute();
   }

   @Override
   public RetryAwareCompletionStage<MetadataValue<V>> getWithMetadataAsync(K key, SocketAddress preferredAddres) {
      assertRemoteCacheManagerIsStarted();
      GetWithMetadataOperation<V> op = operationsFactory.newGetWithMetadataOperation(
            keyAsObjectIfNeeded(key), keyToBytes(key), dataFormat, preferredAddres);
      return op.internalExecute();
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to putAll entries (%s) lifespan:%d (%s), maxIdle:%d (%s)", map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
      Map<byte[], byte[]> byteMap = new HashMap<>();
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         byteMap.put(keyToBytes(entry.getKey()), valueToBytes(entry.getValue()));
      }
      PutAllParallelOperation op = operationsFactory.newPutAllOperation(byteMap, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, dataFormat);
      return op.execute();
   }

   @Override
   public CompletableFuture<Long> sizeAsync() {
      assertRemoteCacheManagerIsStarted();
      SizeOperation op = operationsFactory.newSizeOperation();
      return op.execute().thenApply(Integer::longValue);
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   @Override
   public ClientStatistics clientStatistics() {
      return clientStatistics;
   }

   @Override
   public ServerStatistics serverStatistics() {
      assertRemoteCacheManagerIsStarted();
      StatsOperation op = operationsFactory.newStatsOperation();
      Map<String, String> statsMap = await(op.execute());
      ServerStatisticsImpl stats = new ServerStatisticsImpl();
      for (Map.Entry<String, String> entry : statsMap.entrySet()) {
         stats.addStats(entry.getKey(), entry.getValue());
      }
      return stats;
   }

   @Override
   public K keyAsObjectIfNeeded(Object key) {
      return isObjectStorage ? (K) key : null;
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to add (K,V): (%s, %s) lifespan:%d, maxIdle:%d", key, value, lifespan, maxIdleTime);
      }
      PutOperation<V> op = operationsFactory.newPutKeyValueOperation(keyAsObjectIfNeeded(key),
            keyToBytes(key), valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, dataFormat);
      return op.execute();
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      assertRemoteCacheManagerIsStarted();
      ClearOperation op = operationsFactory.newClearOperation();
      return op.execute();
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      CompletableFuture<MetadataValue<V>> cf = getWithMetadataAsync(key);

      return cf.thenCompose(metadataValue -> {
         V newValue;
         V oldValue;
         long version;
         if (metadataValue != null) {
            oldValue = metadataValue.getValue();
            version = metadataValue.getVersion();
         } else {
            oldValue = null;
            version = -1;
         }
         newValue = remappingFunction.apply(key, oldValue);

         CompletionStage<Boolean> doneStage;
         if (newValue != null) {
            if (oldValue != null) {
               doneStage = replaceWithVersionAsync(key, newValue, version, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
            } else {
               doneStage = putIfAbsentAsync(key, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit)
                  .thenApply(Objects::isNull);
            }
         } else {
            if (oldValue != null) {
               doneStage = removeWithVersionAsync(key, version);
            } else {
               // Nothing to remove
               doneStage = CompletableFuture.completedFuture(Boolean.TRUE);
            }
         }

         return doneStage.thenCompose(done -> {
            if (done) {
               return CompletableFuture.completedFuture(newValue);
            }
            // Retry if one of the operations failed
            return computeAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
         });
      });
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      PutIfAbsentOperation<V> op = operationsFactory.newPutIfAbsentOperation(keyAsObjectIfNeeded(key),
            keyToBytes(key), valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, dataFormat);
      return op.execute();
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Objects.requireNonNull(oldValue);
      Objects.requireNonNull(newValue);
      CompletionStage<MetadataValue<V>> stage = getWithMetadataAsync(key);
      return stage.thenCompose(metadataValue -> {
         if (metadataValue != null) {
            V prevValue = metadataValue.getValue();
            if (oldValue.equals(prevValue)) {
               return replaceWithVersionAsync(key, newValue, metadataValue.getVersion(), lifespan, lifespanUnit, maxIdle, maxIdleUnit)
                     .thenCompose(replaced -> {
                        if (replaced) {
                           return CompletableFuture.completedFuture(replaced);
                        }
                        // Concurrent modification - the value could still equal - we need to retry
                        return replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
                     });
            }
         }
         return CompletableFuture.completedFuture(Boolean.FALSE);
      }).toCompletableFuture();
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      assertRemoteCacheManagerIsStarted();
      RemoveOperation<V> removeOperation = operationsFactory.newRemoveOperation(keyAsObjectIfNeeded(key), keyToBytes(key), dataFormat);
      // TODO: It sucks that you need the prev value to see if it works...
      // We need to find a better API for RemoteCache...
      return removeOperation.execute();
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      Objects.requireNonNull(value);
      CompletionStage<MetadataValue<V>> stage = getWithMetadataAsync((K) key);
      return stage.thenCompose(metadataValue -> {
         if (metadataValue == null || !value.equals(metadataValue.getValue())) {
            return CompletableFuture.completedFuture(Boolean.FALSE);
         }
         return removeWithVersionAsync((K) key, metadataValue.getVersion())
               .thenCompose(removed -> {
                  if (removed) {
                     return CompletableFuture.completedFuture(Boolean.TRUE);
                  }
                  // Concurrent operation - need to retry
                  return removeAsync(key, value);
               });
      }).toCompletableFuture();
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      ReplaceOperation<V> op = operationsFactory.newReplaceOperation(keyAsObjectIfNeeded(key),
            keyToBytes(key), valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, dataFormat);
      return op.execute();
   }

   @Override
   public CompletableFuture<Boolean> containsKeyAsync(K key) {
      assertRemoteCacheManagerIsStarted();
      ContainsKeyOperation op = operationsFactory.newContainsKeyOperation(
            keyAsObjectIfNeeded(key), keyToBytes(key), dataFormat);
      return op.execute();
   }

   @Override
   public boolean containsValue(Object value) {
      Objects.requireNonNull(value);
      return values().contains(value);
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to getAll entries (%s)", keys);
      }
      Set<byte[]> byteKeys = new HashSet<>(keys.size());
      for (Object key : keys) {
         byteKeys.add(keyToBytes(key));
      }
      GetAllParallelOperation<K, V> op = operationsFactory.newGetAllOperation(byteKeys, dataFormat);
      return op.execute().thenApply(Collections::unmodifiableMap);
   }

   @Override
   public void start() {
      if (log.isDebugEnabled()) {
         log.debugf("Start called, nothing to do here(%s)", getName());
      }
   }

   @Override
   public void stop() {
      unregisterMBean();
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String getVersion() {
      return RemoteCacheImpl.class.getPackage().getImplementationVersion();
   }

   @Override
   public String getProtocolVersion() {
      return "HotRod client, protocol version: " + ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
   }

   @Override
   public void addClientListener(Object listener) {
      assertRemoteCacheManagerIsStarted();
      AddClientListenerOperation op = operationsFactory.newAddClientListenerOperation(listener, dataFormat);
      // no timeout, see below
      await(op.execute());
   }

   @Override
   public void addClientListener(Object listener, Object[] filterFactoryParams, Object[] converterFactoryParams) {
      assertRemoteCacheManagerIsStarted();
      byte[][] marshalledFilterParams = marshallParams(filterFactoryParams);
      byte[][] marshalledConverterParams = marshallParams(converterFactoryParams);
      AddClientListenerOperation op = operationsFactory.newAddClientListenerOperation(
            listener, marshalledFilterParams, marshalledConverterParams, dataFormat);
      // No timeout: transferring initial state can take a while, socket timeout setting is not applicable here
      await(op.execute());
   }

   @Override
   public SocketAddress addNearCacheListener(Object listener, int bloomBits) {
      throw new UnsupportedOperationException("Adding a near cache listener to a RemoteCache is not supported!");
   }

   private byte[][] marshallParams(Object[] params) {
      if (params == null)
         return org.infinispan.commons.util.Util.EMPTY_BYTE_ARRAY_ARRAY;

      byte[][] marshalledParams = new byte[params.length][];
      for (int i = 0; i < marshalledParams.length; i++) {
         byte[] bytes = keyToBytes(params[i]);// should be small
         marshalledParams[i] = bytes;
      }

      return marshalledParams;
   }

   @Override
   public void removeClientListener(Object listener) {
      assertRemoteCacheManagerIsStarted();
      RemoveClientListenerOperation op = operationsFactory.newRemoveClientListenerOperation(listener);
      await(op.execute());
   }

   @Deprecated
   @Override
   public Set<Object> getListeners() {
      ClientListenerNotifier listenerNotifier = operationsFactory.getListenerNotifier();
      return listenerNotifier.getListeners(operationsFactory.getCacheName());
   }

   @Override
   public InternalRemoteCache<K, V> withFlags(Flag... flags) {
      operationsFactory.setFlags(flags);
      return this;
   }

   @Override
   public CompletableFuture<V> getAsync(Object key) {
      assertRemoteCacheManagerIsStarted();
      byte[] keyBytes = keyToBytes(key);
      GetOperation<V> gco = operationsFactory.newGetKeyOperation(keyAsObjectIfNeeded(key), keyBytes, dataFormat);
      CompletableFuture<V> result = gco.execute();
      if (trace) {
         result.thenAccept(value -> log.tracef("For key(%s) returning %s", key, value));
      }

      return result;
   }

   public CompletionStage<PingResponse> ping() {
      return operationsFactory.newFaultTolerantPingOperation().execute();
   }

   @Override
   public byte[] keyToBytes(Object o) {
      return dataFormat.keyToBytes(o);
   }

   protected byte[] valueToBytes(Object o) {
      return dataFormat.valueToBytes(o);
   }

   protected void assertRemoteCacheManagerIsStarted() {
      if (!remoteCacheManager.isStarted()) {
         String message = "Cannot perform operations on a cache associated with an unstarted RemoteCacheManager. Use RemoteCacheManager.start before using the remote cache.";
         HOTROD.unstartedRemoteCacheManager();
         throw new RemoteCacheManagerNotStartedException(message);
      }
   }

   @Override
   public CloseableIteratorSet<K> keySet(IntSet segments) {
      return new RemoteCacheKeySet<>(this, segments);
   }

   @Override
   public CloseableIterator<K> keyIterator(IntSet segments) {
      return operationsFactory.getCodec().keyIterator(this, operationsFactory, segments, batchSize);
   }

   @Override
   public CloseableIteratorSet<Entry<K, V>> entrySet(IntSet segments) {
      return new RemoteCacheEntrySet<>(this, segments);
   }

   @Override
   public CloseableIterator<Entry<K, V>> entryIterator(IntSet segments) {
      return operationsFactory.getCodec().entryIterator(this, segments, batchSize);
   }

   @Override
   public CloseableIteratorCollection<V> values(IntSet segments) {
      return new RemoteCacheValuesCollection<>(this, segments);
   }

   @Override
   public <T> T execute(String taskName, Map<String, ?> params) {
      return execute(taskName, params, null);
   }

   @Override
   public <T> T execute(String taskName, Map<String, ?> params, Object key) {
      assertRemoteCacheManagerIsStarted();
      Map<String, byte[]> marshalledParams = new HashMap<>();
      if (params != null) {
         for (java.util.Map.Entry<String, ?> entry : params.entrySet()) {
            marshalledParams.put(entry.getKey(), keyToBytes(entry.getValue()));
         }
      }
      Object keyHint = null;
      if (key != null) {
         keyHint = isObjectStorage ? key : keyToBytes(key);
      }
      ExecuteOperation<T> op = operationsFactory.newExecuteOperation(taskName, marshalledParams, keyHint, dataFormat);
      return await(op.execute());
   }

   @Override
   public CacheTopologyInfo getCacheTopologyInfo() {
      return operationsFactory.getCacheTopologyInfo();
   }

   @Override
   public StreamingRemoteCache<K> streaming() {
      assertRemoteCacheManagerIsStarted();
      return new StreamingRemoteCacheImpl<>(this);
   }

   @Override
   public <T, U> InternalRemoteCache<T, U> withDataFormat(DataFormat newDataFormat) {
      Objects.requireNonNull(newDataFormat, "Data Format must not be null")
            .initialize(remoteCacheManager, isObjectStorage);
      RemoteCacheImpl<T, U> instance = newInstance();
      instance.dataFormat = newDataFormat;
      return instance;
   }

   private <T, U> RemoteCacheImpl<T, U> newInstance() {
      RemoteCacheImpl<T, U> copy = new RemoteCacheImpl<>(this.remoteCacheManager, name, clientStatistics);
      copy.init(this.defaultMarshaller, this.operationsFactory, this.batchSize);
      return copy;
   }

   public void resolveStorage(boolean objectStorage) {
      this.isObjectStorage = objectStorage;
      this.defaultDataFormat.initialize(remoteCacheManager, isObjectStorage);
   }

   @Override
   public DataFormat getDataFormat() {
      return dataFormat;
   }

   @Override
   public boolean isTransactional() {
      return false;
   }

   public boolean isObjectStorage() {
      return isObjectStorage;
   }

   @Override
   public boolean hasForceReturnFlag() {
      return operationsFactory.hasFlag(Flag.FORCE_RETURN_VALUE);
   }

   @Override
   public CompletionStage<Void> updateBloomFilter() {
      return CompletableFuture.completedFuture(null);
   }
}
