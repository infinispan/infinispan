package org.infinispan.client.hotrod.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.client.hotrod.filter.Filters.makeFactoryParams;
import static org.infinispan.client.hotrod.impl.Util.await;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.StreamingRemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.StatisticsConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.client.hotrod.impl.iteration.RemoteCloseableIterator;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.ClearOperation;
import org.infinispan.client.hotrod.impl.operations.ContainsKeyOperation;
import org.infinispan.client.hotrod.impl.operations.ExecuteOperation;
import org.infinispan.client.hotrod.impl.operations.GetAllParallelOperation;
import org.infinispan.client.hotrod.impl.operations.GetOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithVersionOperation;
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
import org.infinispan.client.hotrod.impl.operations.SizeOperation;
import org.infinispan.client.hotrod.impl.operations.StatsOperation;
import org.infinispan.client.hotrod.jmx.RemoteCacheClientStatisticsMXBean;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.query.dsl.Query;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> extends RemoteCacheSupport<K, V> {

   private static final Log log = LogFactory.getLog(RemoteCacheImpl.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private Marshaller defaultMarshaller;
   private final String name;
   private final RemoteCacheManager remoteCacheManager;
   protected OperationsFactory operationsFactory;
   private int estimateKeySize;
   private int estimateValueSize;
   private int batchSize;
   private volatile boolean isObjectStorage;
   private final DataFormat defaultDataFormat;
   private DataFormat dataFormat;
   protected ClientStatistics clientStatistics;
   private ObjectName mbeanObjectName;

   public RemoteCacheImpl(RemoteCacheManager rcm, String name, TimeService timeService) {
      if (trace) {
         log.tracef("Creating remote cache: %s", name);
      }
      this.name = name;
      this.remoteCacheManager = rcm;
      this.defaultDataFormat = DataFormat.builder().build();
      this.clientStatistics = new ClientStatistics(rcm.getConfiguration().statistics().enabled(), timeService);
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

   public void init(Marshaller marshaller, OperationsFactory operationsFactory,
                    int estimateKeySize, int estimateValueSize, int batchSize,
                    ObjectName jmxParent) {
      this.defaultMarshaller = marshaller;
      this.operationsFactory = operationsFactory;
      this.estimateKeySize = estimateKeySize;
      this.estimateValueSize = estimateValueSize;
      this.batchSize = batchSize;
      this.dataFormat = defaultDataFormat;
      registerMBean(jmxParent);
   }

   /**
    * Inititalize without mbeans
    */
   public void init(Marshaller marshaller, OperationsFactory operationsFactory,
                    int estimateKeySize, int estimateValueSize, int batchSize) {
      this.defaultMarshaller = marshaller;
      this.operationsFactory = operationsFactory;
      this.estimateKeySize = estimateKeySize;
      this.estimateValueSize = estimateValueSize;
      this.batchSize = batchSize;
      this.dataFormat = defaultDataFormat;
   }

   public ClientStatistics getClientStatistics() {
      return clientStatistics;
   }

   private void registerMBean(ObjectName jmxParent) {
      try {
         StatisticsConfiguration configuration = getRemoteCacheManager().getConfiguration().statistics();
         if (configuration.jmxEnabled()) {
            MBeanServer mbeanServer = configuration.mbeanServerLookup().getMBeanServer();
            String groupName = String.format("type=HotRodClient,name=%s", configuration.jmxName());
            mbeanObjectName = new ObjectName(String.format("%s:%s,cache=%s", jmxParent.getDomain(), groupName, name.equals("") ? "org.infinispan.default" : name));
            JmxUtil.registerMBean(clientStatistics, mbeanObjectName, mbeanServer);
         }
      } catch (Exception e) {
         log.warn("MBean registration failed", e);
      }
   }

   private void unregisterMBean() {
      try {
         if (mbeanObjectName != null) {
            StatisticsConfiguration configuration = getRemoteCacheManager().getConfiguration().statistics();
            MBeanServer mbeanServer = configuration.mbeanServerLookup().getMBeanServer();
            JmxUtil.unregisterMBean(mbeanObjectName, mbeanServer);
         }
      } catch (Exception e) {
         log.warn("MBean unregistration failed", e);
      }
   }

   public OperationsFactory getOperationsFactory() {
      return operationsFactory;
   }

   @Override
   public RemoteCacheManager getRemoteCacheManager() {
      return remoteCacheManager;
   }

   @Override
   public boolean removeWithVersion(K key, long version) {
      return await(removeWithVersionAsync(key, version));
   }

   @Override
   public CompletableFuture<Boolean> removeWithVersionAsync(final K key, final long version) {
      assertRemoteCacheManagerIsStarted();
      RemoveIfUnmodifiedOperation<V> op = operationsFactory.newRemoveIfUnmodifiedOperation(
            keyAsObjectIfNeeded(key), keyToBytes(key), version, dataFormat);
      return op.execute().thenApply(response -> response.getCode().isUpdated());
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds, int maxIdleTimeSeconds) {
      return replaceWithVersion(key, newValue, version, lifespanSeconds, TimeUnit.SECONDS, maxIdleTimeSeconds, TimeUnit.SECONDS);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return await(replaceWithVersionAsync(key, newValue, version, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit));
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds, int maxIdleSeconds) {
      return replaceWithVersionAsync(key, newValue, version, lifespanSeconds, TimeUnit.SECONDS, maxIdleSeconds, TimeUnit.SECONDS);
   }

   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      ReplaceIfUnmodifiedOperation op = operationsFactory.newReplaceIfUnmodifiedOperation(
            keyAsObjectIfNeeded(key), keyToBytes(key), valueToBytes(newValue), lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version, dataFormat);
      return op.execute().thenApply(response -> response.getCode().isUpdated());
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      assertRemoteCacheManagerIsStarted();
      if (segments != null && segments.isEmpty()) {
         return Closeables.iterator(Collections.emptyIterator());
      }
      byte[][] params = marshallParams(filterConverterParams);
      RemoteCloseableIterator remoteCloseableIterator = new RemoteCloseableIterator(operationsFactory,
            defaultMarshaller, filterConverterFactory, params, segments, batchSize, false, dataFormat);
      remoteCloseableIterator.start();
      return remoteCloseableIterator;
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Set<Integer> segments, int batchSize) {
      return retrieveEntries(filterConverterFactory, null, segments, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, int batchSize) {
      return retrieveEntries(filterConverterFactory, null, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntriesByQuery(Query filterQuery, Set<Integer> segments, int batchSize) {
      Object[] factoryParams = makeFactoryParams(filterQuery);
      return retrieveEntries(Filters.ITERATION_QUERY_FILTER_CONVERTER_FACTORY_NAME, factoryParams, segments, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, MetadataValue<Object>>> retrieveEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      RemoteCloseableIterator remoteCloseableIterator = new RemoteCloseableIterator(operationsFactory, defaultMarshaller, batchSize, segments, true, dataFormat);
      remoteCloseableIterator.start();
      return remoteCloseableIterator;
   }

   @Override
   public VersionedValue<V> getVersioned(K key) {
      assertRemoteCacheManagerIsStarted();
      if (ConfigurationProperties.isVersionPre12(remoteCacheManager.getConfiguration())) {
         GetWithVersionOperation<V> op = operationsFactory.newGetWithVersionOperation(
               keyAsObjectIfNeeded(key), keyToBytes(key), dataFormat);
         return await(op.execute());
      } else {
         MetadataValue<V> result = getWithMetadata(key);
         return result != null
               ? new VersionedValueImpl<>(result.getVersion(), result.getValue())
               : null;
      }
   }

   @Override
   public CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key) {
      return getWithMetadataAsync(key, dataFormat);
   }

   @Override
   public MetadataValue<V> getWithMetadata(K key) {
      return await(getWithMetadataAsync(key));
   }

   private CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key, DataFormat dataFormat) {
      assertRemoteCacheManagerIsStarted();
      GetWithMetadataOperation<V> op = operationsFactory.newGetWithMetadataOperation(
            keyAsObjectIfNeeded(key), keyToBytes(key), dataFormat);
      return op.execute();
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      await(putAllAsync(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
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
   public int size() {
      assertRemoteCacheManagerIsStarted();
      SizeOperation op = operationsFactory.newSizeOperation();
      return await(op.execute());
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   @Override
   public RemoteCacheClientStatisticsMXBean clientStatistics() {
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
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(putAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   K keyAsObjectIfNeeded(Object key) {
      return isObjectStorage ? (K) key : null;
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(replaceAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      VersionedValue<V> versionedValue = getWithMetadata(key);
      return versionedValue != null && versionedValue.getValue().equals(oldValue) &&
            replaceWithVersion(key, value, versionedValue.getVersion(), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
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
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      PutIfAbsentOperation<V> op = operationsFactory.newPutIfAbsentOperation(keyAsObjectIfNeeded(key),
            keyToBytes(key), valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, dataFormat);
      return op.execute();
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
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      ReplaceOperation<V> op = operationsFactory.newReplaceOperation(keyAsObjectIfNeeded(key),
            keyToBytes(key), valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, dataFormat);
      return op.execute();
   }

   @Override
   public boolean containsKey(Object key) {
      assertRemoteCacheManagerIsStarted();
      ContainsKeyOperation op = operationsFactory.newContainsKeyOperation(
            keyAsObjectIfNeeded(key), keyToBytes(key), dataFormat);
      return await(op.execute());
   }

   @Override
   public boolean containsValue(Object value) {
      Objects.requireNonNull(value);
      return values().contains(value);
   }

   @Override
   public V get(Object key) {
      return await(getAsync(key));
   }

   @Override
   public Map<K, V> getAll(Set<? extends K> keys) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to getAll entries (%s)", keys);
      }
      Set<byte[]> byteKeys = new HashSet<>(keys.size());
      for (K key : keys) {
         byteKeys.add(keyToBytes(key));
      }
      GetAllParallelOperation<K, V> op = operationsFactory.newGetAllOperation(byteKeys, dataFormat);
      return await(op.execute().thenApply(Collections::unmodifiableMap));
   }

   @Override
   public V remove(Object key) {
      return await(removeAsync(key));
   }

   @Override
   public boolean remove(Object key, Object value) {
      return removeEntry((K) key, (V) value);
   }

   @Override
   public void clear() {
      await(clearAsync());
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

   @Override
   public Set<Object> getListeners() {
      ClientListenerNotifier listenerNotifier = operationsFactory.getListenerNotifier();
      return listenerNotifier.getListeners(operationsFactory.getCacheName());
   }

   @Override
   public RemoteCache<K, V> withFlags(Flag... flags) {
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

   public PingResponse ping() {
      return await(operationsFactory.newFaultTolerantPingOperation().execute());
   }

   protected byte[] keyToBytes(Object o) {
      return dataFormat.keyToBytes(o, estimateKeySize, estimateValueSize);
   }

   protected byte[] valueToBytes(Object o) {
      return dataFormat.valueToBytes(o, estimateKeySize, estimateValueSize);
   }

   protected void assertRemoteCacheManagerIsStarted() {
      if (!remoteCacheManager.isStarted()) {
         String message = "Cannot perform operations on a cache associated with an unstarted RemoteCacheManager. Use RemoteCacheManager.start before using the remote cache.";
         if (log.isInfoEnabled()) {
            log.unstartedRemoteCacheManager();
         }
         throw new RemoteCacheManagerNotStartedException(message);
      }
   }

   @Override
   protected void set(K key, V value) {
      // no need to optimize the put operation: all invocations are already non-return by default,
      // see org.infinispan.client.hotrod.Flag.FORCE_RETURN_VALUE
      // Warning: never invoke put(K,V) in this scope or we'll get a stackoverflow.
      put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public CloseableIteratorSet<K> keySet() {
      return keySet(null);
   }

   @Override
   public CloseableIteratorSet<K> keySet(IntSet segments) {
      return new KeySet(segments);
   }

   @Override
   public CloseableIteratorSet<Entry<K, V>> entrySet() {
      return entrySet(null);
   }

   @Override
   public CloseableIteratorSet<Entry<K, V>> entrySet(IntSet segments) {
      return new EntrySet(segments);
   }

   @Override
   public CloseableIteratorCollection<V> values() {
      return values(null);
   }

   @Override
   public CloseableIteratorCollection<V> values(IntSet segments) {
      return new ValuesCollection(segments);
   }

   private class KeySet extends AbstractSet<K> implements CloseableIteratorSet<K> {
      private final IntSet segments;

      private KeySet(IntSet segments) {
         this.segments = segments;
      }

      @Override
      public CloseableIterator<K> iterator() {
         CloseableIterator<K> keyIterator = operationsFactory.getCodec().keyIterator(RemoteCacheImpl.this, operationsFactory,
               segments, batchSize);
         return new RemovableCloseableIterator<>(keyIterator, this::remove);
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         return Closeables.spliterator(iterator(), Long.MAX_VALUE, Spliterator.NONNULL | Spliterator.CONCURRENT |
               Spliterator.DISTINCT);
      }

      @Override
      public Stream<K> stream() {
         return Closeables.stream(spliterator(), false);
      }

      @Override
      public Stream<K> parallelStream() {
         return Closeables.stream(spliterator(), true);
      }

      @Override
      public int size() {
         return RemoteCacheImpl.this.size();
      }

      @Override
      public void clear() {
         RemoteCacheImpl.this.clear();
      }

      @Override
      public boolean contains(Object o) {
         return RemoteCacheImpl.this.containsKey(o);
      }

      @Override
      public boolean remove(Object o) {
         return RemoteCacheImpl.this.remove(o) != null;
      }

      @Override
      public boolean removeAll(Collection<?> c) {
         boolean removedSomething = false;
         for (Object key : c) {
            removedSomething |= remove(key);
         }
         return removedSomething;
      }
   }

   private boolean removeEntry(Map.Entry<K, V> entry) {
      return removeEntry(entry.getKey(), entry.getValue());
   }

   private boolean removeEntry(K key, V value) {
      VersionedValue<V> versionedValue = getWithMetadata(key);
      return versionedValue != null && value.equals(versionedValue.getValue()) &&
            RemoteCacheImpl.this.removeWithVersion(key, versionedValue.getVersion());
   }

   private class EntrySet extends AbstractSet<Entry<K, V>> implements CloseableIteratorSet<Entry<K, V>> {
      private final IntSet segments;

      public EntrySet(IntSet segments) {
         this.segments = segments;
      }

      @Override
      public CloseableIterator<Entry<K, V>> iterator() {
         return new RemovableCloseableIterator<>(operationsFactory.getCodec().entryIterator(RemoteCacheImpl.this,
               segments, batchSize), this::remove);
      }

      @Override
      public CloseableSpliterator<Entry<K, V>> spliterator() {
         return Closeables.spliterator(iterator(), Long.MAX_VALUE, Spliterator.NONNULL | Spliterator.CONCURRENT);
      }

      @Override
      public Stream<Entry<K, V>> stream() {
         return Closeables.stream(spliterator(), false);
      }

      @Override
      public Stream<Entry<K, V>> parallelStream() {
         return Closeables.stream(spliterator(), true);
      }

      @Override
      public int size() {
         return RemoteCacheImpl.this.size();
      }

      @Override
      public void clear() {
         RemoteCacheImpl.this.clear();
      }

      @Override
      public boolean contains(Object o) {
         Map.Entry entry = toEntry(o);
         if (entry != null) {
            V value = RemoteCacheImpl.this.get(entry.getKey());
            return value != null && value.equals(entry.getValue());
         }
         return false;
      }

      @Override
      public boolean remove(Object o) {
         Map.Entry<K, V> entry = toEntry(o);
         return entry != null && RemoteCacheImpl.this.removeEntry(entry);
      }

      private Map.Entry<K, V> toEntry(Object obj) {
         if (obj instanceof Map.Entry) {
            return (Map.Entry) obj;
         } else {
            return null;
         }
      }
   }

   private class ValuesCollection extends AbstractCollection<V> implements CloseableIteratorCollection<V> {
      private final IntSet segments;

      private ValuesCollection(IntSet segments) {
         this.segments = segments;
      }

      @Override
      public CloseableIterator<V> iterator() {
         CloseableIterator<Map.Entry<K, V>> entryIterator = operationsFactory.getCodec().entryIterator(
               RemoteCacheImpl.this, segments, batchSize);
         return new IteratorMapper<>(new RemovableCloseableIterator<>(entryIterator, e -> RemoteCacheImpl.this.remove(e.getKey(), e.getValue())),
               // Convert to V for user
               Entry::getValue);
      }

      @Override
      public CloseableSpliterator<V> spliterator() {
         return Closeables.spliterator(iterator(), Long.MAX_VALUE, Spliterator.NONNULL | Spliterator.CONCURRENT);
      }

      @Override
      public Stream<V> stream() {
         return Closeables.stream(spliterator(), false);
      }

      @Override
      public Stream<V> parallelStream() {
         return Closeables.stream(spliterator(), true);
      }

      @Override
      public int size() {
         return RemoteCacheImpl.this.size();
      }

      @Override
      public void clear() {
         RemoteCacheImpl.this.clear();
      }

      @Override
      public boolean contains(Object o) {
         // TODO: This would be more efficient if done on the server as a task or separate protocol
         // Have to close the stream, just in case stream terminates early
         try (Stream<V> stream = stream()) {
            return stream.anyMatch(v -> Objects.deepEquals(v, o));
         }
      }

      // This method can terminate early so we have to make sure to close iterator
      @Override
      public boolean remove(Object o) {
         Objects.requireNonNull(o);
         try (CloseableIterator<V> iter = iterator()) {
            while (iter.hasNext()) {
               if (o.equals(iter.next())) {
                  iter.remove();
                  return true;
               }
            }
         }
         return false;
      }
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
   public <T, U> RemoteCache<T, U> withDataFormat(DataFormat newDataFormat) {
      newDataFormat = Objects.requireNonNull(newDataFormat, "Data Format must not be null");
      newDataFormat.initialize(remoteCacheManager, isObjectStorage);
      RemoteCacheImpl<T, U> instance = newInstance();
      instance.dataFormat = newDataFormat;
      return instance;
   }

   private <T, U> RemoteCacheImpl<T, U> newInstance() {
      RemoteCacheImpl<T, U> copy = new RemoteCacheImpl<>(this.remoteCacheManager, name, clientStatistics);
      copy.init(this.defaultMarshaller, this.operationsFactory, this.estimateKeySize, this.estimateValueSize, this.batchSize);
      return copy;
   }

   public PingResponse resolveStorage() {
      if (remoteCacheManager.isStarted()) {
         PingResponse result = ping();
         this.isObjectStorage = operationsFactory.getCodec().isObjectStorageHinted(result);
         this.defaultDataFormat.initialize(remoteCacheManager, isObjectStorage);
         return result;
      }
      return PingResponse.EMPTY;
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
}
