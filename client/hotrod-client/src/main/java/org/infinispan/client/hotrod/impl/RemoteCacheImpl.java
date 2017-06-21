package org.infinispan.client.hotrod.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.client.hotrod.filter.Filters.makeFactoryParams;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.StreamingRemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.event.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.client.hotrod.impl.iteration.RemoteCloseableIterator;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.BulkGetOperation;
import org.infinispan.client.hotrod.impl.operations.ClearOperation;
import org.infinispan.client.hotrod.impl.operations.ContainsKeyOperation;
import org.infinispan.client.hotrod.impl.operations.ExecuteOperation;
import org.infinispan.client.hotrod.impl.operations.GetAllParallelOperation;
import org.infinispan.client.hotrod.impl.operations.GetOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithVersionOperation;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingOperation;
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
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.query.dsl.Query;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> extends RemoteCacheSupport<K, V> {

   private static final Log log = LogFactory.getLog(RemoteCacheImpl.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private Marshaller marshaller;
   private final String name;
   private final RemoteCacheManager remoteCacheManager;
   private volatile ExecutorService executorService;
   protected OperationsFactory operationsFactory;
   private int estimateKeySize;
   private int estimateValueSize;
   private int batchSize;
   private volatile boolean hasCompatibility;

   private final Runnable clear = this::clear;

   public RemoteCacheImpl(RemoteCacheManager rcm, String name) {
      if (trace) {
         log.tracef("Creating remote cache: %s", name);
      }
      this.name = name;
      this.remoteCacheManager = rcm;
   }

   public void init(Marshaller marshaller, ExecutorService executorService, OperationsFactory operationsFactory,
         int estimateKeySize, int estimateValueSize, int batchSize) {
      this.marshaller = marshaller;
      this.executorService = executorService;
      this.operationsFactory = operationsFactory;
      this.estimateKeySize = estimateKeySize;
      this.estimateValueSize = estimateValueSize;
      this.batchSize = batchSize;
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
      assertRemoteCacheManagerIsStarted();
      RemoveIfUnmodifiedOperation<V> op = operationsFactory.newRemoveIfUnmodifiedOperation(
         compatKeyIfNeeded(key), obj2bytes(key, true), version);
      VersionedOperationResponse<V> response = op.execute();
      return response.getCode().isUpdated();
   }

   @Override
   public CompletableFuture<Boolean> removeWithVersionAsync(final K key, final long version) {
      assertRemoteCacheManagerIsStarted();
      return CompletableFuture.supplyAsync(() -> removeWithVersion(key, version), executorService);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds, int maxIdleTimeSeconds) {
      return replaceWithVersion(key, newValue, version, lifespanSeconds, TimeUnit.SECONDS, maxIdleTimeSeconds, TimeUnit.SECONDS);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      ReplaceIfUnmodifiedOperation op = operationsFactory.newReplaceIfUnmodifiedOperation(
         compatKeyIfNeeded(key), obj2bytes(key, true), obj2bytes(newValue, false), lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version);
      VersionedOperationResponse response = op.execute();
      return response.getCode().isUpdated();
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(final K key, final V newValue, final long version, final int lifespanSeconds, final int maxIdleSeconds) {
      assertRemoteCacheManagerIsStarted();
      return CompletableFuture.supplyAsync(() ->
              replaceWithVersion(key, newValue, version, lifespanSeconds, maxIdleSeconds), executorService);
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      assertRemoteCacheManagerIsStarted();
      if (segments != null && segments.isEmpty()) {
         return Closeables.iterator(Collections.emptyIterator());
      }
      byte[][] params = marshallParams(filterConverterParams);
      RemoteCloseableIterator remoteCloseableIterator = new RemoteCloseableIterator(operationsFactory,
              filterConverterFactory, params, segments, batchSize, false);
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
      RemoteCloseableIterator remoteCloseableIterator = new RemoteCloseableIterator(operationsFactory, batchSize, segments, true);
      remoteCloseableIterator.start();
      return remoteCloseableIterator;
   }

   @Override
   public VersionedValue<V> getVersioned(K key) {
      assertRemoteCacheManagerIsStarted();
      if (ConfigurationProperties.isVersionPre12(remoteCacheManager.getConfiguration())) {
         GetWithVersionOperation<V> op = operationsFactory.newGetWithVersionOperation(
               compatKeyIfNeeded(key), obj2bytes(key, true));
         return op.execute();
      } else {
         MetadataValue<V> result = getWithMetadata(key);
         return result != null
               ? new VersionedValueImpl<>(result.getVersion(), result.getValue())
               : null;
      }
   }

   @Override
   public MetadataValue<V> getWithMetadata(K key) {
      assertRemoteCacheManagerIsStarted();
      GetWithMetadataOperation<V> op = operationsFactory.newGetWithMetadataOperation(
         compatKeyIfNeeded(key), obj2bytes(key, true));
      return op.execute();
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to putAll entries (%s) lifespan:%d (%s), maxIdle:%d (%s)", map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
      Map<byte[], byte[]> byteMap = new HashMap<>();
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         byteMap.put(obj2bytes(entry.getKey(),  true), obj2bytes(entry.getValue(), false));
      }
      PutAllParallelOperation op = operationsFactory.newPutAllOperation(byteMap, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      op.execute();
   }

   @Override
   public CompletableFuture<Void> putAllAsync(final Map<? extends K, ? extends V> data, final long lifespan, final TimeUnit lifespanUnit, final long maxIdle, final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      return CompletableFuture.supplyAsync(() -> {
         putAll(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
         return null;
      }, executorService);
   }

   @Override
   public int size() {
      assertRemoteCacheManagerIsStarted();
      SizeOperation op = operationsFactory.newSizeOperation();
      return op.execute();
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   @Override
   public ServerStatistics stats() {
      assertRemoteCacheManagerIsStarted();
      StatsOperation op = operationsFactory.newStatsOperation();
      Map<String, String> statsMap = op.execute();
      ServerStatisticsImpl stats = new ServerStatisticsImpl();
      for (Map.Entry<String, String> entry : statsMap.entrySet()) {
         stats.addStats(entry.getKey(), entry.getValue());
      }
      return stats;
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to add (K,V): (%s, %s) lifespan:%d, maxIdle:%d", key, value, lifespan, maxIdleTime);
      }
      PutOperation<V> op = operationsFactory.newPutKeyValueOperation(compatKeyIfNeeded(key),
         obj2bytes(key, true), obj2bytes(value, false), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return op.execute();
   }

   K compatKeyIfNeeded(Object key) {
      return hasCompatibility ? (K) key : null;
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      PutIfAbsentOperation<V> op = operationsFactory.newPutIfAbsentOperation(compatKeyIfNeeded(key),
         obj2bytes(key, true), obj2bytes(value, false), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return op.execute();
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      ReplaceOperation<V> op = operationsFactory.newReplaceOperation(compatKeyIfNeeded(key),
         obj2bytes(key, true), obj2bytes(value, false), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return op.execute();
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      VersionedValue<V> versionedValue = getWithMetadata(key);
      return versionedValue != null && versionedValue.getValue().equals(oldValue) &&
            replaceWithVersion(key, value, versionedValue.getVersion(), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public CompletableFuture<V> putAsync(final K key, final V value, final long lifespan, final TimeUnit lifespanUnit, final long maxIdle, final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      int flags = operationsFactory.flags();
      return CompletableFuture.supplyAsync(() -> {
         if (flags != 0)
            operationsFactory.setFlags(flags);
         return put(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      }, executorService);
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      assertRemoteCacheManagerIsStarted();
      return CompletableFuture.runAsync(clear, executorService);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(final K key,final V value,final long lifespan,final TimeUnit lifespanUnit,final long maxIdle,final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      int flags = operationsFactory.flags();
      return CompletableFuture.supplyAsync(() -> {
         if (flags != 0)
            operationsFactory.setFlags(flags);
         return putIfAbsent(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      }, executorService);
   }

   @Override
   public CompletableFuture<V> removeAsync(final Object key) {
      assertRemoteCacheManagerIsStarted();
      int flags = operationsFactory.flags();
      return CompletableFuture.supplyAsync(() -> {
         if (flags != 0)
            operationsFactory.setFlags(flags);
         return remove(key);
      }, executorService);
   }

   @Override
   public CompletableFuture<V> replaceAsync(final K key,final V value,final long lifespan,final TimeUnit lifespanUnit,final long maxIdle,final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      int flags = operationsFactory.flags();
      return CompletableFuture.supplyAsync(() -> {
         if (flags != 0)
            operationsFactory.setFlags(flags);
         return replace(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      }, executorService);
   }

   @Override
   public boolean containsKey(Object key) {
      assertRemoteCacheManagerIsStarted();
      ContainsKeyOperation op = operationsFactory.newContainsKeyOperation(
         compatKeyIfNeeded(key), obj2bytes(key, true));
      return op.execute();
   }

   @Override
   public boolean containsValue(Object value) {
      Objects.requireNonNull(value);
      return values().contains(value);
   }

   @Override
   public V get(Object key) {
      assertRemoteCacheManagerIsStarted();
      byte[] keyBytes = obj2bytes(key, true);
      GetOperation<V> gco = operationsFactory.newGetKeyOperation(compatKeyIfNeeded(key), keyBytes);
      V result = gco.execute();
      if (trace) {
         log.tracef("For key(%s) returning %s", key, result);
      }
      return result;
   }

   @Override
   public Map<K, V> getAll(Set<? extends K> keys) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to getAll entries (%s)", keys);
      }
      Set<byte[]> byteKeys = new HashSet<>(keys.size());
      for (K key : keys) {
         byteKeys.add(obj2bytes(key, true));
      }
      GetAllParallelOperation<K, V> op = operationsFactory.newGetAllOperation(byteKeys);
      Map<K, V> result = op.execute();
      return Collections.unmodifiableMap(result);
   }

   @Override
   public Map<K, V> getBulk() {
      return getBulk(0);
   }

   @Override
   public Map<K, V> getBulk(int size) {
      assertRemoteCacheManagerIsStarted();
      BulkGetOperation<K, V> op = operationsFactory.newBulkGetOperation(size);
      Map<K, V> result = op.execute();
      return Collections.unmodifiableMap(result);
   }

   @Override
   public V remove(Object key) {
      assertRemoteCacheManagerIsStarted();
      RemoveOperation<V> removeOperation = operationsFactory.newRemoveOperation(compatKeyIfNeeded(key), obj2bytes(key, true));
      // TODO: It sucks that you need the prev value to see if it works...
      // We need to find a better API for RemoteCache...
      return removeOperation.execute();
   }

   @Override
   public boolean remove(Object key, Object value) {
      return removeEntry((K) key, (V) value);
   }

   @Override
   public void clear() {
      assertRemoteCacheManagerIsStarted();
      ClearOperation op = operationsFactory.newClearOperation() ;
      op.execute();
   }

   @Override
   public void start() {
      if (log.isDebugEnabled()) {
         log.debugf("Start called, nothing to do here(%s)", getName());
      }
   }

   @Override
   public void stop() {
      if (log.isDebugEnabled()) {
         log.debugf("Stop called, nothing to do here(%s)", getName());
      }
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
      AddClientListenerOperation op = operationsFactory.newAddClientListenerOperation(listener);
      op.execute();
   }

   @Override
   public void addClientListener(Object listener, Object[] filterFactoryParams, Object[] converterFactoryParams) {
      assertRemoteCacheManagerIsStarted();
      byte[][] marshalledFilterParams = marshallParams(filterFactoryParams);
      byte[][] marshalledConverterParams = marshallParams(converterFactoryParams);
      AddClientListenerOperation op = operationsFactory.newAddClientListenerOperation(
            listener, marshalledFilterParams, marshalledConverterParams);
      op.execute();
   }

   private byte[][] marshallParams(Object[] params) {
      if (params == null)
         return new byte[0][];

      byte[][] marshalledParams = new byte[params.length][];
      for (int i = 0; i < marshalledParams.length; i++) {
         byte[] bytes = obj2bytes(params[i], true);// should be small
         marshalledParams[i] = bytes;
      }

      return marshalledParams;
   }

   @Override
   public void removeClientListener(Object listener) {
      assertRemoteCacheManagerIsStarted();
      RemoveClientListenerOperation op = operationsFactory.newRemoveClientListenerOperation(listener);
      op.execute();
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
   public CompletableFuture<V> getAsync(final K key) {
      assertRemoteCacheManagerIsStarted();
      return CompletableFuture.supplyAsync(() -> get(key), executorService);
   }

   public PingOperation.PingResult ping() {
      return operationsFactory.newFaultTolerantPingOperation().execute();
   }

   byte[] obj2bytes(Object o, boolean isKey) {
      try {
         return marshaller.objectToByteBuffer(o, isKey ? estimateKeySize : estimateValueSize);
      } catch (IOException ioe) {
         throw new HotRodClientException(
               "Unable to marshall object of type [" + o.getClass().getName() + "]", ioe);
      } catch (InterruptedException ie) {
         Thread.currentThread().interrupt();
         return null;
      }
   }

   private void assertRemoteCacheManagerIsStarted() {
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
      return new KeySet();
   }

   @Override
   public CloseableIteratorSet<Entry<K, V>> entrySet() {
      return new EntrySet();
   }

   @Override
   public CloseableIteratorCollection<V> values() {
      return new ValuesCollection();
   }

   private class KeySet extends AbstractCollection<K> implements CloseableIteratorSet<K> {

      @Override
      public CloseableIterator<K> iterator() {
         return new RemovableCloseableIterator<>(
               // Use the ToEmptyBytesKeyValueFilterConverter to reduce value payload
               new CloseableIteratorMapper<>(retrieveEntries(
                     "org.infinispan.server.hotrod.HotRodServer$ToEmptyBytesKeyValueFilterConverter",
                     batchSize), e -> (K) e.getKey()),
               RemoteCacheImpl.this::remove);
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

   private CloseableIterator<Entry<K, VersionedValue<V>>> castIterator(CloseableIterator iterator) {
      return iterator;
   }

   private boolean removeEntry(Map.Entry<K, V> entry) {
      return removeEntry(entry.getKey(), entry.getValue());
   }

   private boolean removeEntry(K key, V value) {
      VersionedValue<V> versionedValue = getWithMetadata(key);
      return versionedValue != null && value.equals(versionedValue.getValue()) &&
            RemoteCacheImpl.this.removeWithVersion(key, versionedValue.getVersion());
   }

   private class EntrySet extends AbstractCollection<Map.Entry<K, V>> implements CloseableIteratorSet<Entry<K, V>> {

      @Override
      public CloseableIterator<Entry<K, V>> iterator() {
         return new CloseableIteratorMapper<>(new RemovableCloseableIterator<>(
               // First make <K, VersionedValue<V>>
               castIterator(retrieveEntriesWithMetadata(null, batchSize)),
               // Apply remove using version
               e -> removeWithVersion(e.getKey(), e.getValue().getVersion())),
               // Convert to <K, V> for user
               e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue().getValue()));
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

      @Override
      public CloseableIterator<V> iterator() {
         return new CloseableIteratorMapper<>(new RemovableCloseableIterator<>(
               // First make <K, VersionedValue<V>>
               castIterator(retrieveEntriesWithMetadata(null, batchSize)),
               // Apply remove using version
               e -> removeWithVersion(e.getKey(), e.getValue().getVersion())),
               // Convert to V for user
               e -> e.getValue().getValue());
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
            return stream.anyMatch(v -> Objects.equals(v, o));
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
      assertRemoteCacheManagerIsStarted();
      Map<String, byte[]> marshalledParams = new HashMap<>();
      if (params != null) {
         for(java.util.Map.Entry<String, ?> entry : params.entrySet()) {
            marshalledParams.put(entry.getKey(), obj2bytes(entry.getValue(), false));
         }
      }
      ExecuteOperation<T> op = operationsFactory.newExecuteOperation(taskName, marshalledParams);
      return op.execute();
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

   public PingOperation.PingResult resolveCompatibility() {
      if (remoteCacheManager.isStarted()) {
         PingOperation.PingResult result = ping();
         hasCompatibility = result.hasCompatibility();
         return result;
      }

      return PingOperation.PingResult.FAIL;
   }

   private abstract class WithFlagsCallable implements Callable<V> {
      final int intFlags;

      protected WithFlagsCallable(int intFlags) {
         this.intFlags = intFlags;
      }

      void setFlagsIfPresent() {
         if (intFlags != 0)
            operationsFactory.setFlags(intFlags);
      }
   }

}
