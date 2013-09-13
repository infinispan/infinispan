package org.infinispan.client.hotrod.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.Version;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.impl.async.NotifyingFutureImpl;
import org.infinispan.client.hotrod.impl.operations.BulkGetKeysOperation;
import org.infinispan.client.hotrod.impl.operations.BulkGetOperation;
import org.infinispan.client.hotrod.impl.operations.ClearOperation;
import org.infinispan.client.hotrod.impl.operations.ContainsKeyOperation;
import org.infinispan.client.hotrod.impl.operations.GetOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithVersionOperation;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingOperation;
import org.infinispan.client.hotrod.impl.operations.PutIfAbsentOperation;
import org.infinispan.client.hotrod.impl.operations.PutOperation;
import org.infinispan.client.hotrod.impl.operations.RemoveIfUnmodifiedOperation;
import org.infinispan.client.hotrod.impl.operations.RemoveOperation;
import org.infinispan.client.hotrod.impl.operations.ReplaceIfUnmodifiedOperation;
import org.infinispan.client.hotrod.impl.operations.ReplaceOperation;
import org.infinispan.client.hotrod.impl.operations.StatsOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.concurrent.NotifyingFuture;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> extends RemoteCacheSupport<K, V> {

   private static final Log log = LogFactory.getLog(RemoteCacheImpl.class, Log.class);

   private Marshaller marshaller;
   private final String name;
   private final RemoteCacheManager remoteCacheManager;
   private volatile ExecutorService executorService;
   private OperationsFactory operationsFactory;
   private int estimateKeySize;
   private int estimateValueSize;


   public RemoteCacheImpl(RemoteCacheManager rcm, String name) {
      if (log.isTraceEnabled()) {
         log.tracef("Creating remote cache: %s", name);
      }
      this.name = name;
      this.remoteCacheManager = rcm;
   }

   public void init(Marshaller marshaller, ExecutorService executorService, OperationsFactory operationsFactory, int estimateKeySize, int estimateValueSize) {
      this.marshaller = marshaller;
      this.executorService = executorService;
      this.operationsFactory = operationsFactory;
      this.estimateKeySize = estimateKeySize;
      this.estimateValueSize = estimateValueSize;
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
      RemoveIfUnmodifiedOperation op = operationsFactory.newRemoveIfUnmodifiedOperation(obj2bytes(key, true), version);
      VersionedOperationResponse response = op.execute();
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> removeWithVersionAsync(final K key, final long version) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Boolean> result = new NotifyingFutureImpl<Boolean>();
      Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            boolean removed = removeWithVersion(key, version);
            result.notifyFutureCompletion();
            return removed;
         }
      });
      result.setExecuting(future);
      return result;
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds, int maxIdleTimeSeconds) {
      assertRemoteCacheManagerIsStarted();
      ReplaceIfUnmodifiedOperation op = operationsFactory.newReplaceIfUnmodifiedOperation(obj2bytes(key, true), obj2bytes(newValue, false), lifespanSeconds, maxIdleTimeSeconds, version);
      VersionedOperationResponse response = op.execute();
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(final K key, final V newValue, final long version, final int lifespanSeconds, final int maxIdleSeconds) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Boolean> result = new NotifyingFutureImpl<Boolean>();
      Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            boolean removed = replaceWithVersion(key, newValue, version, lifespanSeconds, maxIdleSeconds);
            result.notifyFutureCompletion();
            return removed;
         }
      });
      result.setExecuting(future);
      return result;
   }

   @Override
   public VersionedValue<V> getVersioned(K key) {
      assertRemoteCacheManagerIsStarted();
      GetWithVersionOperation op = operationsFactory.newGetWithVersionOperation(obj2bytes(key, true));
      VersionedValue<byte[]> value = op.execute();
      return binary2VersionedValue(value);
   }

   @Override
   public MetadataValue<V> getWithMetadata(K key) {
      assertRemoteCacheManagerIsStarted();
      GetWithMetadataOperation op = operationsFactory.newGetWithMetadataOperation(obj2bytes(key, true));
      MetadataValue<byte[]> value = op.execute();
      return binary2MetadataValue(value);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         put(entry.getKey(), entry.getValue(), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(final Map<? extends K, ? extends V> data, final long lifespan, final TimeUnit lifespanUnit, final long maxIdle, final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Void> result = new NotifyingFutureImpl<Void>();
      Future<Void> future = executorService.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            putAll(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
            result.notifyFutureCompletion();
            return null;
         }
      });
      result.setExecuting(future);
      return result;

   }

   @Override
   public int size() {
      assertRemoteCacheManagerIsStarted();
      StatsOperation op = operationsFactory.newStatsOperation();
      return Integer.parseInt(op.execute().get(ServerStatistics.CURRENT_NR_OF_ENTRIES));
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
   @SuppressWarnings("unchecked")
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      applyDefaultExpirationFlags(lifespan, maxIdleTime);
      if (log.isTraceEnabled()) {
         log.tracef("About to add (K,V): (%s, %s) lifespanSecs:%d, maxIdleSecs:%d", key, value, lifespanSecs, maxIdleSecs);
      }
      PutOperation op = operationsFactory.newPutKeyValueOperation(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs);
      byte[] result = op.execute();
      return (V) bytes2obj(result);
   }


   @Override
   @SuppressWarnings("unchecked")
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      applyDefaultExpirationFlags(lifespan, maxIdleTime);
      PutIfAbsentOperation op = operationsFactory.newPutIfAbsentOperation(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs);
      byte[] bytes = op.execute();
      return (V) bytes2obj(bytes);
   }

   @Override
   @SuppressWarnings("unchecked")
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      applyDefaultExpirationFlags(lifespan, maxIdleTime);
      ReplaceOperation op = operationsFactory.newReplaceOperation(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs);
      byte[] bytes = op.execute();
      return (V) bytes2obj(bytes);
   }

   @Override
   public NotifyingFuture<V> putAsync(final K key, final V value, final long lifespan, final TimeUnit lifespanUnit, final long maxIdle, final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            V prevValue = put(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
            result.notifyFutureCompletion();
            return prevValue;
         }
      });
      result.setExecuting(future);
      return result;
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Void> result = new NotifyingFutureImpl<Void>();
      Future<Void> future = executorService.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            clear();
            result.notifyFutureCompletion();
            return null;
         }
      });
      result.setExecuting(future);
      return result;
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(final K key,final V value,final long lifespan,final TimeUnit lifespanUnit,final long maxIdle,final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            V prevValue = putIfAbsent(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
            result.notifyFutureCompletion();
            return prevValue;
         }
      });
      result.setExecuting(future);
      return result;
   }

   @Override
   public NotifyingFuture<V> removeAsync(final Object key) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            V toReturn = remove(key);
            result.notifyFutureCompletion();
            return toReturn;
         }
      });
      result.setExecuting(future);
      return result;
   }

   @Override
   public NotifyingFuture<V> replaceAsync(final K key,final V value,final long lifespan,final TimeUnit lifespanUnit,final long maxIdle,final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            V v = replace(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
            result.notifyFutureCompletion();
            return v;
         }
      });
      result.setExecuting(future);
      return result;
   }

   @Override
   public boolean containsKey(Object key) {
      assertRemoteCacheManagerIsStarted();
      ContainsKeyOperation op = operationsFactory.newContainsKeyOperation(obj2bytes(key, true));
      return op.execute();
   }

   @Override
   @SuppressWarnings("unchecked")
   public V get(Object key) {
      assertRemoteCacheManagerIsStarted();
      byte[] keyBytes = obj2bytes(key, true);
      GetOperation gco = operationsFactory.newGetKeyOperation(keyBytes);
      byte[] bytes = gco.execute();
      V result = (V) bytes2obj(bytes);
      if (log.isTraceEnabled()) {
         log.tracef("For key(%s) returning %s", key, result);
      }
      return result;
   }

   @Override
   public Map<K, V> getBulk() {
      return getBulk(0);
   }

   @Override
   @SuppressWarnings("unchecked")
   public Map<K, V> getBulk(int size) {
      assertRemoteCacheManagerIsStarted();
      BulkGetOperation op = operationsFactory.newBulkGetOperation(size);
      Map<byte[], byte[]> result = op.execute();
      Map<K,V> toReturn = new HashMap<K,V>();
      for (Map.Entry<byte[], byte[]> entry : result.entrySet()) {
         V value = (V) bytes2obj(entry.getValue());
         K key = (K) bytes2obj(entry.getKey());
         toReturn.put(key, value);
      }
      return Collections.unmodifiableMap(toReturn);
   }

   @Override
   @SuppressWarnings("unchecked")
   public V remove(Object key) {
      assertRemoteCacheManagerIsStarted();
      RemoveOperation removeOperation = operationsFactory.newRemoveOperation(obj2bytes(key, true));
      byte[] existingValue = removeOperation.execute();
      // TODO: It sucks that you need the prev value to see if it works...
      // We need to find a better API for RemoteCache...
      return (V) bytes2obj(existingValue);
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
      return Version.getProtocolVersion();
   }

   @Override
   public RemoteCache<K, V> withFlags(Flag... flags) {
      operationsFactory.setFlags(flags);
      return this;
   }

   @Override
   public NotifyingFuture<V> getAsync(final K key) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            V toReturn = get(key);
            result.notifyFutureCompletion();
            return toReturn;
         }
      });
      result.setExecuting(future);
      return result;
   }

   public PingOperation.PingResult ping() {
      return operationsFactory.newFaultTolerantPingOperation().execute();
   }

   private byte[] obj2bytes(Object o, boolean isKey) {
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

   private Object bytes2obj(byte[] bytes) {
      if (bytes == null) return null;
      try {
         return marshaller.objectFromByteBuffer(bytes);
      } catch (Exception e) {
         throw new HotRodClientException(
               "Unable to unmarshall byte stream", e);
      }
   }

   @SuppressWarnings("unchecked")
   private VersionedValue<V> binary2VersionedValue(VersionedValue<byte[]> value) {
      if (value == null)
         return null;
      V valueObj = (V) bytes2obj(value.getValue());
      return new VersionedValueImpl<V>(value.getVersion(), valueObj);
   }

   @SuppressWarnings("unchecked")
   private MetadataValue<V> binary2MetadataValue(MetadataValue<byte[]> value) {
      if (value == null)
         return null;
      V valueObj = (V) bytes2obj(value.getValue());
      return new MetadataValueImpl<V>(value.getCreated(), value.getLifespan(), value.getLastUsed(), value.getMaxIdle(), value.getVersion(), valueObj);
   }

   private int toSeconds(long duration, TimeUnit timeUnit) {
      return (int) timeUnit.toSeconds(duration);
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

   private void applyDefaultExpirationFlags(long lifespan, long maxIdle) {
      if (lifespan == 0) {
         operationsFactory.addFlags(Flag.DEFAULT_LIFESPAN);
      }
      if (maxIdle == 0) {
         operationsFactory.addFlags(Flag.DEFAULT_MAXIDLE);
      }
   }

   @Override
   public Set<K> keySet() {
	   assertRemoteCacheManagerIsStarted();
	   // Use default scope
	   BulkGetKeysOperation op = operationsFactory.newBulkGetKeysOperation(0);
	   Set<byte[]> result = op.execute();
       Set<K> toReturn = new HashSet<K>();
       for (byte[] keyBytes : result) {
          K key = (K) bytes2obj(keyBytes);
          toReturn.add(key);
       }
       return Collections.unmodifiableSet(toReturn);
   }
}
