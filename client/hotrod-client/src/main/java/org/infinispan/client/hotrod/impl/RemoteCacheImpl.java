package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.filter.Filters.makeFactoryParams;
import static org.infinispan.client.hotrod.impl.Util.await;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.ObjectName;

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
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCacheContainer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.StreamingRemoteCache;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.StatisticsConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.client.hotrod.impl.cache.CacheEntryImpl;
import org.infinispan.client.hotrod.impl.cache.CacheEntryMetadataImpl;
import org.infinispan.client.hotrod.impl.cache.CacheEntryVersionImpl;
import org.infinispan.client.hotrod.impl.iteration.RemotePublisher;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.AdvancedHotRodOperation;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.GetAllBulkOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.operations.PingResponse;
import org.infinispan.client.hotrod.impl.operations.PutAllBulkOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec30;
import org.infinispan.client.hotrod.impl.query.RemoteQueryFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.commons.api.query.ContinuousQuery;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;

import io.netty.channel.Channel;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> extends RemoteCacheSupport<K, V> implements InternalRemoteCache<K, V> {

   private static final Log log = LogFactory.getLog(RemoteCacheImpl.class, Log.class);

   protected final String name;
   protected final byte[] nameBytes;
   protected final RemoteCacheManager remoteCacheManager;
   protected final CacheOperationsFactory operationsFactory;
   protected final ClientListenerNotifier clientListenerNotifier;
   protected final int flagInt;
   protected int batchSize;
   protected DataFormat dataFormat;
   protected ClientStatistics clientStatistics;
   protected ObjectName mbeanObjectName;
   protected OperationDispatcher dispatcher;
   protected final RemoteQueryFactory queryFactory;

   public RemoteCacheImpl(RemoteCacheManager rcm, String name, TimeService timeService,
                          Function<InternalRemoteCache<K,V>, CacheOperationsFactory> factoryFunction) {
      this(rcm, name, timeService, null, factoryFunction);
   }

   public RemoteCacheImpl(RemoteCacheManager rcm, String name, TimeService timeService, NearCacheService<K, V> nearCacheService,
                          Function<InternalRemoteCache<K,V>, CacheOperationsFactory> factoryFunction) {
      if (log.isTraceEnabled()) {
         log.tracef("Creating remote cache: %s", name);
      }
      this.name = name;
      this.nameBytes = name.getBytes(StandardCharsets.UTF_8);
      this.remoteCacheManager = rcm;
      this.dataFormat = DataFormat.builder().build();
      this.clientStatistics = new ClientStatistics(timeService, nearCacheService, rcm.getConfiguration().metricRegistry().withCache(name));
      this.operationsFactory = factoryFunction.apply(this);
      this.clientListenerNotifier = rcm.getListenerNotifier();
      this.flagInt = rcm.getConfiguration().forceReturnValues() ? Flag.FORCE_RETURN_VALUE.getFlagInt() : 0;
      this.queryFactory = new RemoteQueryFactory(this);
   }

   protected RemoteCacheImpl(RemoteCacheImpl<?, ?> other, int flagInt) {
      if (log.isTraceEnabled()) {
         log.tracef("Creating remote cache: %s with flags %d", other.name, flagInt);
      }
      this.name = other.name;
      this.nameBytes = other.nameBytes;
      this.remoteCacheManager = other.remoteCacheManager;
      this.dataFormat = other.dataFormat;
      this.clientStatistics = other.clientStatistics;
      this.operationsFactory = other.operationsFactory.newFactoryFor(this);
      this.flagInt = flagInt;
      this.clientListenerNotifier = other.clientListenerNotifier;

      // set the values for init
      this.batchSize = other.batchSize;
      this.dispatcher = other.dispatcher;
      this.queryFactory = other.queryFactory;
   }

   @Override
   public void init(Configuration configuration, OperationDispatcher dispatcher, ObjectName jmxParent) {
      init(configuration, dispatcher);
      if (jmxParent != null) {
         registerMBean(jmxParent);
      }
   }

   @Override
   public OperationDispatcher getDispatcher() {
      return dispatcher;
   }

   /**
    * Inititalize without mbeans
    */
   public void init(Configuration configuration, OperationDispatcher dispatcher) {
      init(configuration.batchSize(), dispatcher);
   }

   private void init(int batchSize, OperationDispatcher dispatcher) {
      this.batchSize = batchSize;
      this.dispatcher = dispatcher;
   }

   private void registerMBean(ObjectName jmxParent) {
      StatisticsConfiguration configuration = getRemoteCacheContainer().getConfiguration().statistics();
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

   private synchronized void unregisterMBean() {
      if (mbeanObjectName != null) {
         try {
            MBeanServer mBeanServer = getRemoteCacheContainer().getConfiguration().statistics()
                  .mbeanServerLookup().getMBeanServer();
            if (mBeanServer.isRegistered(mbeanObjectName)) {
               mBeanServer.unregisterMBean(mbeanObjectName);
            } else {
               HOTROD.debugf("MBean not registered: %s", mbeanObjectName);
            }
            mbeanObjectName = null;
         } catch (Exception e) {
            throw HOTROD.jmxUnregistrationFailure(e);
         }
      }
   }

   @Override
   public RemoteCacheContainer getRemoteCacheContainer() {
      return remoteCacheManager;
   }

   @Override
   public CompletableFuture<Boolean> removeWithVersionAsync(final K key, final long version) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<VersionedOperationResponse<V>> op = operationsFactory.newRemoveIfUnmodifiedOperation(key, version);
      return dispatcher.execute(op)
            .thenApply(response -> response.getCode().isUpdated())
            .toCompletableFuture();
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      throw new UnsupportedOperationException();
   }

   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<VersionedOperationResponse<V>> op = operationsFactory.newReplaceIfUnmodifiedOperation(
            key, newValue, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version);
      return dispatcher.execute(op)
            .thenApply(response -> response.getCode().isUpdated())
            .toCompletableFuture();
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
      return new RemotePublisher<>(operationsFactory, dispatcher, filterConverterFactory, filterConverterParams, segments,
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
      return new RemotePublisher<>(operationsFactory, dispatcher, null, null, segments,
            batchSize, true, dataFormat);
   }

   @Override
   public CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key) {
      assertRemoteCacheManagerIsStarted();
      var op = operationsFactory.<K, V>newGetWithMetadataOperation(key, null);
      return dispatcher.execute(op)
            .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value)
            .toCompletableFuture();
   }

   @Override
   public CompletionStage<GetWithMetadataOperation.GetWithMetadataResult<V>> getWithMetadataAsync(K key, Channel channel) {
      assertRemoteCacheManagerIsStarted();
      var op = operationsFactory.<K, V>newGetWithMetadataOperation(key, channel);
      return channel != null ?
            dispatcher.executeOnSingleAddress(op, ChannelRecord.of(channel)) :
            dispatcher.execute(op);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      if (log.isTraceEnabled()) {
         log.tracef("About to putAll entries (%s) lifespan:%d (%s), maxIdle:%d (%s)", map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
      var op = new PutAllBulkOperation(map, dataFormat,
            serialized -> operationsFactory.newPutAllBytesOperation(serialized, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
      return dispatcher.executeBulk(name, op).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Long> sizeAsync() {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<Integer> op = operationsFactory.newSizeOperation();
      return dispatcher.execute(op)
            .toCompletableFuture()
            .thenApply(Integer::longValue);
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
      return await(serverStatisticsAsync());
   }

   @Override
   public CompletionStage<ServerStatistics> serverStatisticsAsync() {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<ServerStatistics> op = operationsFactory.newStatsOperation();
      return dispatcher.execute(op).toCompletableFuture();
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      if (log.isTraceEnabled()) {
         log.tracef("About to add (K,V): (%s, %s) lifespan:%d, maxIdle:%d", key, value, lifespan, maxIdleTime);
      }
      HotRodOperation<MetadataValue<V>> op = operationsFactory.newPutKeyValueOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return dispatcher.execute(op)
            .thenApply(CacheEntryConversion.extractValue())
            .toCompletableFuture();
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<Void> op = operationsFactory.newClearOperation();
      return dispatcher.execute(op).toCompletableFuture();
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
               doneStage = putIfAbsentAsync(key, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit, Flag.FORCE_RETURN_VALUE)
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
      CompletableFuture<V> cf = getAsync(key);
      return cf.thenCompose(oldValue -> {
         if (oldValue != null) return CompletableFuture.completedFuture(oldValue);

         V newValue = mappingFunction.apply(key);
         if (newValue == null) return CompletableFutures.completedNull();

         return putIfAbsentAsync(key, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit)
               .thenApply(v -> v == null ? newValue : v);
      });
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      CompletableFuture<MetadataValue<V>> cf = getWithMetadataAsync(key);
      return cf.thenCompose(metadata -> {
         if (metadata == null || metadata.getValue() == null) return CompletableFutures.completedNull();

         V newValue = remappingFunction.apply(key, metadata.getValue());
         CompletableFuture<Boolean> done;
         if (newValue == null) {
            done = removeWithVersionAsync(key, metadata.getVersion());
         } else {
            done = replaceWithVersionAsync(key, newValue, metadata.getVersion(), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
         }

         return done.thenCompose(success -> {
            if (success) {
               return CompletableFuture.completedFuture(newValue);
            }

            return computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
         });
      });
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<MetadataValue<V>> op = operationsFactory.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return dispatcher.execute(op)
            .thenApply(CacheEntryConversion.extractValue())
            .toCompletableFuture();
   }

   private CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag ... flags) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<MetadataValue<V>> op = operationsFactory.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
      return dispatcher.execute(op)
            .thenApply(CacheEntryConversion.extractValue())
            .toCompletableFuture();
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
      HotRodOperation<MetadataValue<V>> removeOperation = operationsFactory.newRemoveOperation(key);
      // TODO: It sucks that you need the prev value to see if it works...
      // We need to find a better API for RemoteCache...
      return dispatcher.execute(removeOperation)
            .thenApply(CacheEntryConversion.extractValue())
            .toCompletableFuture();
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
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
                                            TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<V> op = operationsFactory.newReplaceOperation(key, value, lifespan, lifespanUnit, maxIdleTime,
            maxIdleTimeUnit);
      return dispatcher.execute(op).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Boolean> containsKeyAsync(K key) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<Boolean> op = operationsFactory.newContainsKeyOperation(key);
      return dispatcher.execute(op).toCompletableFuture();
   }

   @Override
   public boolean containsValue(Object value) {
      Objects.requireNonNull(value);
      return values().contains(value);
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      assertRemoteCacheManagerIsStarted();
      if (log.isTraceEnabled()) {
         log.tracef("About to getAll entries (%s)", keys);
      }
      var op = new GetAllBulkOperation<K, V>(keys, dataFormat, operationsFactory::newGetAllBytesOperation);
      return dispatcher.executeBulk(name, op)
            .thenApply(Collections::unmodifiableMap)
            .toCompletableFuture();
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
      remoteCacheManager.getConfiguration().metricRegistry().removeCache(name);
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public byte[] getNameBytes() {
      return nameBytes;
   }

   @Override
   public String getVersion() {
      return RemoteCacheImpl.class.getPackage().getImplementationVersion();
   }

   @Override
   public <T> Query<T> query(String query) {
      return queryFactory.create(query);
   }

   @Override
   public ContinuousQuery<K, V> continuousQuery() {
      return queryFactory.continuousQuery(this);
   }

   @Override
   public String getProtocolVersion() {
      return "HotRod client, protocol version: " + ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
   }

   @Override
   public void addClientListener(Object listener) {
      addClientListener(listener, null, null);
   }

   @Override
   public void addClientListener(Object listener, Object[] filterFactoryParams, Object[] converterFactoryParams) {
      assertRemoteCacheManagerIsStarted();
      AddClientListenerOperation op = operationsFactory.newAddClientListenerOperation(listener, filterFactoryParams, converterFactoryParams);
      // Must be registered before executing to ensure this is always ran on the event loop, thus guaranteeing
      // events cannot be received until after this has been processed
      // We must wait on the stage to ensure the listeners are indeed registered fully before returning
      await(dispatcher.executeAddListener(op));
   }

   @Override
   public Channel addNearCacheListener(Object listener, int bloomBits) {
      throw new UnsupportedOperationException("Adding a near cache listener to a RemoteCache is not supported!");
   }

   @Override
   public void removeClientListener(Object listener) {
      assertRemoteCacheManagerIsStarted();
      byte[] listenerId = clientListenerNotifier.findListenerId(listener);
      if (listenerId == null) {
         return;
      }
      SocketAddress sa = clientListenerNotifier.findAddress(listenerId);
      if (sa == null) {
         return;
      }
      HotRodOperation<Void> op = operationsFactory.newRemoveClientListenerOperation(listener);
      // By registering this here, we are guaranteed this will be ran on the event loop for this listener
      CompletableFuture<Void> removalStage = op.asCompletableFuture().thenAccept(___ -> {
         clientListenerNotifier.removeClientListener(listenerId);
         dispatcher.removeListener(sa, listenerId);
      });
      dispatcher.executeOnSingleAddress(op, sa);
      // This is convoluted but to ensure the caller doesn't return until the listener is completely removed
      // we have to wait on the other stage
      await(removalStage);
   }

   @Override
   public InternalRemoteCache<K, V> withFlags(Flag... flags) {
      if (flags.length == 0) {
         return this;
      }
      int newFlags = 0;
      for (Flag flag : flags) {
         newFlags |= flag.getFlagInt();
      }
      int resultingFlags = (int) EnumUtil.mergeBitSets(flagInt, newFlags);
      if (resultingFlags == flagInt) {
         return this;
      }
      return newInstance(resultingFlags);
   }

   @Override
   public InternalRemoteCache<K, V> noFlags() {
      return newInstance(0);
   }

   @Override
   public Set<Flag> flags() {
      return EnumUtil.enumSetOf(flagInt, Flag.class);
   }

   @Override
   public int flagInt() {
      return flagInt;
   }

   @Override
   public CacheOperationsFactory getOperationsFactory() {
      return operationsFactory;
   }

   @Override
   public ClientListenerNotifier getListenerNotifier() {
      return clientListenerNotifier;
   }

   @Override
   public CompletableFuture<V> getAsync(Object key) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<V> op = operationsFactory.newGetOperation(key);
      if (log.isTraceEnabled()) {
         op.asCompletableFuture().thenAccept(value -> log.tracef("For key(%s) returning %s", key, value));
      }
      return dispatcher.execute(op).toCompletableFuture();
   }

   public CompletionStage<PingResponse> ping() {
      return dispatcher.execute(operationsFactory.newPingOperation())
            .toCompletableFuture();
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
      return new IteratorMapper<>(retrieveEntries(
            // Use the ToEmptyBytesKeyValueFilterConverter to remove value payload
            Codec30.EMPTY_VALUE_CONVERTER, segments, batchSize), e -> (K) e.getKey());
   }

   @Override
   public CloseableIteratorSet<Entry<K, V>> entrySet(IntSet segments) {
      return new RemoteCacheEntrySet<>(this, segments);
   }

   @Override
   public CloseableIterator<Entry<K, V>> entryIterator(IntSet segments) {
      return castEntryIterator(retrieveEntries(null, segments, batchSize));
   }

   protected <K, V> CloseableIterator<Map.Entry<K, V>> castEntryIterator(CloseableIterator iterator) {
      return iterator;
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
      HotRodOperation<T> op = operationsFactory.executeOperation(taskName, marshalledParams, key);
      return await(dispatcher.execute(op));
   }

   @Override
   public CacheTopologyInfo getCacheTopologyInfo() {
      return dispatcher.getCacheTopologyInfo(name);
   }

   @Override
   public StreamingRemoteCache<K> streaming() {
      assertRemoteCacheManagerIsStarted();
      return new StreamingRemoteCacheImpl<>(this);
   }

   @Override
   public <T, U> InternalRemoteCache<T, U> withDataFormat(DataFormat newDataFormat) {
      Objects.requireNonNull(newDataFormat, "Data Format must not be null")
            .initialize(remoteCacheManager, name);
      return newInstance(newDataFormat);
   }

   protected <T, U> InternalRemoteCache<T, U> newInstance(DataFormat dataFormat) {
      RemoteCacheImpl<T,U> instance = new RemoteCacheImpl<>(this, flagInt);
      instance.dataFormat = dataFormat;
      instance.init(batchSize, dispatcher);
      return instance;
   }

   protected <T, U> InternalRemoteCache<T, U> newInstance(int flags) {
      return new RemoteCacheImpl<>(this, flags);
   }

   public void resolveStorage() {
      this.dataFormat.initialize(remoteCacheManager, name);
   }

   @Override
   public void resolveStorage(MediaType key, MediaType value) {
      // Set the storage first and initialize the current data format.
      // We need this to check if the key type match.
      resolveStorage();

      if (key != null && key != MediaType.APPLICATION_UNKNOWN && !dataFormat.getKeyType().match(key)) {
         DataFormat.Builder server = DataFormat.builder()
               .from(this.dataFormat)
               .keyType(key)
               .valueType(value);
         this.dataFormat = DataFormat.builder()
               .from(this.dataFormat)
               .serverDataFormat(server)
               .build();
         resolveStorage();

         // Now proceed and check if the server has an available marshaller.
         // This means that the client DOES NOT have a marshaller capable of converting to the server key type.
         // Therefore, it will utilize the default fallback and NOT convert the object.
         // This could cause additional redirections on the server side and poor performance for the client.
         if (remoteCacheManager.getMarshallerRegistry().getMarshaller(key) == null) {
            log.serverKeyTypeNotRecognized(key);
         }
      }
   }

   @Override
   public DataFormat getDataFormat() {
      return dataFormat;
   }

   @Override
   public boolean isTransactional() {
      return false;
   }

   @Override
   public boolean hasForceReturnFlag() {
      return EnumUtil.containsAny(flagInt, Flag.FORCE_RETURN_VALUE.getFlagInt());
   }

   @Override
   public CompletionStage<Void> updateBloomFilter() {
      return CompletableFuture.completedFuture(null);
   }

   @Override
   public String toString() {
      return "RemoteCache " + name;
   }

   @Override
   public CompletionStage<CacheConfiguration> configuration() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<V> get(K key, CacheOptions options) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<V> op = new AdvancedHotRodOperation<>(operationsFactory.newGetOperation(key), options);
      return dispatcher.execute(op).toCompletableFuture();
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<GetWithMetadataOperation.GetWithMetadataResult<V>> op = operationsFactory.newGetWithMetadataOperation(key, null);
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options))
            .thenApply(GetWithMetadataOperation.GetWithMetadataResult::value)
            .thenApply(CacheEntryConversion.createCacheEntry(key));
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> putIfAbsent(K key, V value, CacheWriteOptions options) {
      assertRemoteCacheManagerIsStarted();
      long lifespan = CacheOptionsUtil.lifespan(options, defaultLifespan, TimeUnit.MILLISECONDS);
      long maxIdle = CacheOptionsUtil.maxIdle(options, defaultMaxIdleTime, TimeUnit.MILLISECONDS);
      HotRodOperation<MetadataValue<V>> op = operationsFactory.newPutIfAbsentOperation(key, value, lifespan, TimeUnit.MILLISECONDS, maxIdle, TimeUnit.MILLISECONDS);
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options, PrivateHotRodFlag.FORCE_RETURN_VALUE.getFlagInt()))
            .thenApply(CacheEntryConversion.createCacheEntry(key));
   }

   @Override
   public CompletionStage<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options) {
      return putIfAbsent(key, value, options).thenApply(e -> e == null || e.value() == null);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> put(K key, V value, CacheWriteOptions options) {
      assertRemoteCacheManagerIsStarted();
      long lifespan = CacheOptionsUtil.lifespan(options, defaultLifespan, TimeUnit.MILLISECONDS);
      long maxIdle = CacheOptionsUtil.maxIdle(options, defaultMaxIdleTime, TimeUnit.MILLISECONDS);
      HotRodOperation<MetadataValue<V>> op = operationsFactory.newPutKeyValueOperation(key, value, lifespan, TimeUnit.MILLISECONDS,
            maxIdle, TimeUnit.MILLISECONDS);
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options, PrivateHotRodFlag.FORCE_RETURN_VALUE.getFlagInt()))
            .thenApply(CacheEntryConversion.createCacheEntry(key));
   }

   @Override
   public CompletionStage<Void> set(K key, V value, CacheWriteOptions options) {
      assertRemoteCacheManagerIsStarted();
      long lifespan = CacheOptionsUtil.lifespan(options, defaultLifespan, TimeUnit.MILLISECONDS);
      long maxIdle = CacheOptionsUtil.maxIdle(options, defaultMaxIdleTime, TimeUnit.MILLISECONDS);
      HotRodOperation<MetadataValue<V>> op = operationsFactory.newPutKeyValueOperation(key, value, lifespan, TimeUnit.MILLISECONDS,
            maxIdle, TimeUnit.MILLISECONDS);
      // By default, does not return previous value.
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options))
            .thenApply(CompletableFutures.toNullFunction());
   }

   @Override
   public CompletionStage<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      assertRemoteCacheManagerIsStarted();
      if (!(Objects.requireNonNull(version) instanceof CacheEntryVersionImpl cevi))
         throw new IllegalArgumentException("Only CacheEntryVersionImpl instances are supported!");

      long lifespan = CacheOptionsUtil.lifespan(options, defaultLifespan, TimeUnit.MILLISECONDS);
      long maxIdle = CacheOptionsUtil.maxIdle(options, defaultMaxIdleTime, TimeUnit.MILLISECONDS);
      HotRodOperation<VersionedOperationResponse<V>> op = operationsFactory.newReplaceIfUnmodifiedOperation(
            key, value, lifespan, TimeUnit.MILLISECONDS, maxIdle, TimeUnit.MILLISECONDS, cevi.version());
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options)).thenApply(r -> r.getCode().isUpdated());
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      assertRemoteCacheManagerIsStarted();
      if (!(Objects.requireNonNull(version) instanceof CacheEntryVersionImpl cevi))
         throw new IllegalArgumentException("Only CacheEntryVersionImpl instances are supported!");

      long lifespan = CacheOptionsUtil.lifespan(options, defaultLifespan, TimeUnit.MILLISECONDS);
      long maxIdle = CacheOptionsUtil.maxIdle(options, defaultMaxIdleTime, TimeUnit.MILLISECONDS);
      var op = operationsFactory.newReplaceIfUnmodifiedOperation(key, value, lifespan, TimeUnit.MILLISECONDS,
            maxIdle, TimeUnit.MILLISECONDS, cevi.version());
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options))
            .thenApply(r -> {
               if (r.getCode().isUpdated()) return null;
               return new CacheEntryImpl<>(key, r.getValue(), new CacheEntryMetadataImpl<V>(r.getMetadata()));
            });
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheOptions options) {
      return getAndRemove(key, options).thenApply(e -> e != null && e.value() != null);
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheEntryVersion version, CacheOptions options) {
      assertRemoteCacheManagerIsStarted();
      if (!(Objects.requireNonNull(version) instanceof CacheEntryVersionImpl cevi))
         throw new IllegalArgumentException("Only CacheEntryVersionImpl instances are supported!");

      var op = operationsFactory.newRemoveIfUnmodifiedOperation(key, cevi.version());
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options))
            .thenApply(res -> res != null && res.getValue() != null);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getAndRemove(K key, CacheOptions options) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<MetadataValue<V>> op = operationsFactory.newRemoveOperation(key);
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options, PrivateHotRodFlag.FORCE_RETURN_VALUE.getFlagInt()))
            .thenApply(CacheEntryConversion.createCacheEntry(key));
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
      assertRemoteCacheManagerIsStarted();
      long lifespan = CacheOptionsUtil.lifespan(options, defaultLifespan, TimeUnit.MILLISECONDS);
      long maxIdle = CacheOptionsUtil.maxIdle(options, defaultMaxIdleTime, TimeUnit.MILLISECONDS);
      if (log.isTraceEnabled()) {
         log.tracef("About to putAll entries (%s) lifespan:%d, maxIdle:%d", entries, lifespan, maxIdle);
      }
      var op = new PutAllBulkOperation(entries, dataFormat,
            serialized -> operationsFactory.newPutAllBytesOperation(serialized, lifespan, TimeUnit.MILLISECONDS, maxIdle, TimeUnit.MILLISECONDS));
      return dispatcher.executeBulk(name, op);
   }

   @Override
   public CompletionStage<Void> putAll(Flow.Publisher<CacheEntry<K, V>> entries, CacheWriteOptions options) {
      return Flowable.fromPublisher(FlowAdapters.toPublisher(entries))
            .collect(Collectors.toMap(CacheEntry::key, CacheEntry::value))
            .concatMapCompletable(map -> Completable.fromCompletionStage(putAll(map, options)))
            .toCompletionStage(null);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options) {
      assertRemoteCacheManagerIsStarted();
      if (log.isTraceEnabled()) {
         log.tracef("About to getAll entries (%s)", keys);
      }
      var op = new GetAllBulkOperation<K, V>(keys, dataFormat, serialized ->
            new AdvancedHotRodOperation<>(operationsFactory.newGetAllBytesOperation(serialized), options));
      CompletionStage<Map<K, V>> cs = dispatcher.executeBulk(name, op)
            .thenApply(Collections::unmodifiableMap);
      // FIXME: getAllOperation doesn't include entry metadata.
      Flowable<CacheEntry<K, V>> flowable = Flowable.defer(() -> Flowable.fromCompletionStage(cs))
            .concatMapIterable(Map::entrySet)
            .map(e -> new CacheEntryImpl<>(e.getKey(), e.getValue(), null));
      return FlowAdapters.toFlowPublisher(flowable);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(CacheOptions options, K[] keys) {
      return getAll(Set.of(keys), options);
   }

   @Override
   public Flow.Publisher<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      return removeAll(Flowable.fromIterable(keys), options);
   }

   @Override
   public Flow.Publisher<K> removeAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return removeAll(Flowable.fromPublisher(FlowAdapters.toPublisher(keys)), options);
   }

   private Flow.Publisher<K> removeAll(Flowable<K> keys, CacheWriteOptions options) {
      assertRemoteCacheManagerIsStarted();
      Flowable<K> flowable = keys.concatMapMaybe(k -> Single.fromCompletionStage(remove(k, options))
            .mapOptional(removed -> removed ? Optional.of(k) : Optional.empty()));
      return FlowAdapters.toFlowPublisher(flowable);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options) {
      return getAndRemoveAll(Flowable.fromIterable(keys), options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return getAndRemoveAll(Flowable.fromPublisher(FlowAdapters.toPublisher(keys)), options);
   }

   private Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flowable<K> keys, CacheWriteOptions options) {
      assertRemoteCacheManagerIsStarted();
      Flowable<CacheEntry<K, V>> flowable = keys
            .concatMapMaybe(k -> Maybe.fromCompletionStage(getAndRemove(k, options)));
      return FlowAdapters.toFlowPublisher(flowable);
   }

   @Override
   public CompletionStage<Long> estimateSize(CacheOptions options) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<Integer> op = operationsFactory.newSizeOperation();
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options))
            .thenApply(Integer::longValue);
   }

   @Override
   public CompletionStage<Void> clear(CacheOptions options) {
      assertRemoteCacheManagerIsStarted();
      HotRodOperation<Void> op = operationsFactory.newClearOperation();
      return dispatcher.execute(new AdvancedHotRodOperation<>(op, options));
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
}
