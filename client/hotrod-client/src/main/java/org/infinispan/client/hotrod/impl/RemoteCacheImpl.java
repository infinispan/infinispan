package org.infinispan.client.hotrod.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
import org.infinispan.client.hotrod.impl.operations.*;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.concurrent.ConvertingNotifyingFuture;
import org.infinispan.commons.util.concurrent.GroupingNotifyingFuture;
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
      return op.executeSync().getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> removeWithVersionAsync(final K key, final long version) {
      assertRemoteCacheManagerIsStarted();
      RemoveIfUnmodifiedOperation op = operationsFactory.newRemoveIfUnmodifiedOperation(obj2bytes(key, true), version);
      return new ConvertingNotifyingFuture<VersionedOperationResponse, Boolean>(op.executeAsync(),
            new ConvertingNotifyingFuture.Converter<VersionedOperationResponse, Boolean>() {
               @Override
               public Boolean convert(VersionedOperationResponse versionedOperationResponse) {
                  return versionedOperationResponse.getCode().isUpdated();
               }
            });
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds, int maxIdleTimeSeconds) {
      assertRemoteCacheManagerIsStarted();
      ReplaceIfUnmodifiedOperation op = operationsFactory.newReplaceIfUnmodifiedOperation(
            obj2bytes(key, true), obj2bytes(newValue, false), lifespanSeconds, maxIdleTimeSeconds, version);
      return op.executeSync().getCode().isUpdated();
   }



   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(final K key, final V newValue, final long version, final int lifespanSeconds, final int maxIdleTimeSeconds) {
      assertRemoteCacheManagerIsStarted();
      ReplaceIfUnmodifiedOperation op = operationsFactory.newReplaceIfUnmodifiedOperation(
            obj2bytes(key, true), obj2bytes(newValue, false), lifespanSeconds, maxIdleTimeSeconds, version);
      return new ConvertingNotifyingFuture<VersionedOperationResponse, Boolean>(op.executeAsync(),
            new ConvertingNotifyingFuture.Converter<VersionedOperationResponse, Boolean>() {
               @Override
               public Boolean convert(VersionedOperationResponse versionedOperationResponse) {
                  return versionedOperationResponse.getCode().isUpdated();
               }
            });
   }

   @Override
   public VersionedValue<V> getVersioned(K key) {
      assertRemoteCacheManagerIsStarted();
      GetWithVersionOperation op = operationsFactory.newGetWithVersionOperation(obj2bytes(key, true));
      return binary2VersionedValue(op.executeSync());
   }

   @Override
   public MetadataValue<V> getWithMetadata(K key) {
      assertRemoteCacheManagerIsStarted();
      GetWithMetadataOperation op = operationsFactory.newGetWithMetadataOperation(obj2bytes(key, true));
      return binary2MetadataValue(op.executeSync());
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      List<NotifyingFuture<V>> futures = new ArrayList<NotifyingFuture<V>>();
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         futures.add(putAsync(entry.getKey(), entry.getValue(), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
      }
      try {
         new GroupingNotifyingFuture<V>(futures).get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new HotRodClientException("Synchronous operation was interrupted", e);
      } catch (ExecutionException e) {
         if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
         } else {
            throw new RuntimeException(e.getCause());
         }
      }
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(final Map<? extends K, ? extends V> map, final long lifespan, final TimeUnit lifespanUnit, final long maxIdleTime, final TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      List<NotifyingFuture<V>> futures = new ArrayList<NotifyingFuture<V>>();
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         futures.add(putAsync(entry.getKey(), entry.getValue(), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
      }
      return new ConvertingNotifyingFuture<Map<NotifyingFuture<V>, V>, Void>(
            new GroupingNotifyingFuture<V>(futures), new ConvertingNotifyingFuture.VoidConverter());
   }

   @Override
   public int size() {
      assertRemoteCacheManagerIsStarted();
      StatsOperation op = operationsFactory.newStatsOperation();
      return Integer.parseInt(op.executeSync().get(ServerStatistics.CURRENT_NR_OF_ENTRIES));
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   @Override
   public ServerStatistics stats() {
      assertRemoteCacheManagerIsStarted();
      StatsOperation op = operationsFactory.newStatsOperation();
      Map<String, String> statsMap = op.executeSync();
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
      return (V) bytes2obj(op.executeSync());
   }


   @Override
   @SuppressWarnings("unchecked")
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      applyDefaultExpirationFlags(lifespan, maxIdleTime);
      PutIfAbsentOperation op = operationsFactory.newPutIfAbsentOperation(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs);
      return (V) bytes2obj(op.executeSync());
   }

   @Override
   @SuppressWarnings("unchecked")
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      applyDefaultExpirationFlags(lifespan, maxIdleTime);
      ReplaceOperation op = operationsFactory.newReplaceOperation(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs);
      return (V) bytes2obj(op.executeSync());
   }

   @Override
   public NotifyingFuture<V> putAsync(final K key, final V value, final long lifespan, final TimeUnit lifespanUnit, final long maxIdleTime, final TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      applyDefaultExpirationFlags(lifespan, maxIdleTime);
      PutOperation op = operationsFactory.newPutKeyValueOperation(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs);
      return new ConvertingNotifyingFuture<byte[], V>(op.executeAsync(), new Bytes2ObjConverter());
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      assertRemoteCacheManagerIsStarted();
      ClearOperation op = operationsFactory.newClearOperation() ;
      return op.executeAsync();
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(final K key,final V value,final long lifespan,final TimeUnit lifespanUnit,final long maxIdleTime, final TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      applyDefaultExpirationFlags(lifespan, maxIdleTime);
      PutIfAbsentOperation op = operationsFactory.newPutIfAbsentOperation(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs);
      return new ConvertingNotifyingFuture<byte[], V>(op.executeAsync(), new Bytes2ObjConverter());
   }

   @Override
   public NotifyingFuture<V> removeAsync(final Object key) {
      assertRemoteCacheManagerIsStarted();
      RemoveOperation op = operationsFactory.newRemoveOperation(obj2bytes(key, true));
      return new ConvertingNotifyingFuture<byte[], V>(op.executeAsync(), new Bytes2ObjConverter());
   }

   @Override
   public NotifyingFuture<V> replaceAsync(final K key,final V value,final long lifespan,final TimeUnit lifespanUnit,final long maxIdleTime,final TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      applyDefaultExpirationFlags(lifespan, maxIdleTime);
      ReplaceOperation op = operationsFactory.newReplaceOperation(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs);
      return new ConvertingNotifyingFuture<byte[], V>(op.executeAsync(), new Bytes2ObjConverter());
   }

   @Override
   public boolean containsKey(Object key) {
      assertRemoteCacheManagerIsStarted();
      ContainsKeyOperation op = operationsFactory.newContainsKeyOperation(obj2bytes(key, true));
      return op.executeSync();
   }

   @Override
   @SuppressWarnings("unchecked")
   public V get(Object key) {
      assertRemoteCacheManagerIsStarted();
      byte[] keyBytes = obj2bytes(key, true);
      GetOperation op = operationsFactory.newGetKeyOperation(keyBytes);
      V result = (V) bytes2obj(op.executeSync());
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
      Map<byte[], byte[]> result = op.executeSync();
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
      RemoveOperation op = operationsFactory.newRemoveOperation(obj2bytes(key, true));
      // TODO: It sucks that you need the prev value to see if it works...
      // We need to find a better API for RemoteCache...
      return (V) bytes2obj(op.executeSync());
   }

   @Override
   public void clear() {
      assertRemoteCacheManagerIsStarted();
      ClearOperation op = operationsFactory.newClearOperation() ;
      op.executeSync();
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
      GetOperation op = operationsFactory.newGetKeyOperation(obj2bytes(key, true));
      return new ConvertingNotifyingFuture<byte[], V>(op.executeAsync(), new Bytes2ObjConverter());
   }

   public PingOperation.PingResult ping() {
      return operationsFactory.newFaultTolerantPingOperation().executeSync();
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
	   Set<byte[]> result = op.executeSync();
       Set<K> toReturn = new HashSet<K>();
       for (byte[] keyBytes : result) {
          K key = (K) bytes2obj(keyBytes);
          toReturn.add(key);
       }
       return Collections.unmodifiableSet(toReturn);
   }


   private class Bytes2ObjConverter implements ConvertingNotifyingFuture.Converter<byte[],V> {
      @Override
      public V convert(byte[] bytes) {
         return (V) bytes2obj(bytes);
      }
   }
}
