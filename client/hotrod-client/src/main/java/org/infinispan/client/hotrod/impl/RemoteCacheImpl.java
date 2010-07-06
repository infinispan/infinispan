package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.HotRodMarshaller;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.Version;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.impl.async.NotifyingFutureImpl;
import org.infinispan.client.hotrod.impl.protocol.HotRodOperations;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> extends RemoteCacheSupport<K, V> {

   private static Log log = LogFactory.getLog(RemoteCacheImpl.class);

   private static final Flag[] FORCE_RETURN_VALUE = {Flag.FORCE_RETURN_VALUE};

   private ThreadLocal<Flag[]> flagsMap = new ThreadLocal<Flag[]>();
   private HotRodOperations operations;
   private HotRodMarshaller marshaller;
   private final String name;
   private final RemoteCacheManager remoteCacheManager;
   private volatile ExecutorService executorService;
   private volatile boolean forceReturnValue;

   public RemoteCacheImpl(RemoteCacheManager rcm, String name, boolean forceReturnValue) {
      if (log.isTraceEnabled()) {
         log.trace("Creating remote cache: " + name);
      }
      this.name = name;
      this.forceReturnValue = forceReturnValue;
      this.remoteCacheManager = rcm;
   }

   public void init(HotRodOperations operations, HotRodMarshaller marshaller, ExecutorService executorService) {
      this.operations = operations;
      this.marshaller = marshaller;
      this.executorService = executorService;
   }

   public RemoteCacheManager getRemoteCacheManager() {
      return remoteCacheManager;
   }

   @Override
   public boolean removeWithVersion(K key, long version) {
      assertRemoteCacheManagerIsStarted();
      VersionedOperationResponse response = operations.removeIfUnmodified(obj2bytes(key, true), version, flags());
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> removeWithVersionAsync(final K key, final long version) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Boolean> result = new NotifyingFutureImpl<Boolean>();
      Future future = executorService.submit(new Callable() {
         @Override
         public Object call() throws Exception {
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
      VersionedOperationResponse response = operations.replaceIfUnmodified(obj2bytes(key, true), obj2bytes(newValue, false), lifespanSeconds, maxIdleTimeSeconds, version, flags());
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(final K key, final V newValue, final long version, final int lifespanSeconds, final int maxIdleSeconds) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Boolean> result = new NotifyingFutureImpl<Boolean>();
      Future future = executorService.submit(new Callable() {
         @Override
         public Object call() throws Exception {
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
      BinaryVersionedValue value = operations.getWithVersion(obj2bytes(key, true), flags());
      return binary2VersionedValue(value);
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
      Future future = executorService.submit(new Callable() {
         @Override
         public Object call() throws Exception {
            putAll(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
            result.notifyFutureCompletion();
            return null;
         }
      });
      result.setExecuting(future);
      return result;

   }

   @Override
   public ServerStatistics stats() {
      assertRemoteCacheManagerIsStarted();
      Map<String, String> statsMap = operations.stats();
      ServerStatisticsImpl stats = new ServerStatisticsImpl();
      for (Map.Entry<String, String> entry : statsMap.entrySet()) {
         stats.addStats(entry.getKey(), entry.getValue());
      }
      return stats;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String getVersion() {
      return Version.getProtocolVersion();
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      if (log.isTraceEnabled()) {
         log.trace("About to add (K,V): (" + key + ", " + value + ") lifespanSecs:" + lifespanSecs + ", maxIdleSecs:" + maxIdleSecs);
      }
      byte[] result = operations.put(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs, flags());
      return (V) bytes2obj(result);
   }


   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      byte[] bytes = operations.putIfAbsent(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs, flags());
      return (V) bytes2obj(bytes);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      byte[] bytes = operations.replace(obj2bytes(key, true), obj2bytes(value, false), lifespanSecs, maxIdleSecs, flags());
      return (V) bytes2obj(bytes);
   }

   @Override
   public NotifyingFuture<V> putAsync(final K key, final V value, final long lifespan, final TimeUnit lifespanUnit, final long maxIdle, final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future future = executorService.submit(new Callable() {
         @Override
         public Object call() throws Exception {
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
      Future future = executorService.submit(new Callable() {
         @Override
         public Object call() throws Exception {
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
      Future future = executorService.submit(new Callable() {
         @Override
         public Object call() throws Exception {
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
      Future future = executorService.submit(new Callable() {
         @Override
         public Object call() throws Exception {
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
      Future future = executorService.submit(new Callable() {
         @Override
         public Object call() throws Exception {
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
      return operations.containsKey(obj2bytes(key, true), flags());
   }

   @Override
   public V get(Object key) {
      assertRemoteCacheManagerIsStarted();
      byte[] bytes = operations.get(obj2bytes(key, true), flags());
      return (V) bytes2obj(bytes);
   }

   @Override
   public V remove(Object key) {
      assertRemoteCacheManagerIsStarted();
      byte[] existingValue = operations.remove(obj2bytes(key, true), flags());
      return (V) bytes2obj(existingValue);
   }

   @Override
   public void clear() {
      assertRemoteCacheManagerIsStarted();
      operations.clear(flags());
   }

   @Override
   public void start() {
      if (log.isInfoEnabled()) {
         log.info("Start called, nothing to do here(" + getName() + ")");
      }
   }

   @Override
   public void stop() {
      if (log.isInfoEnabled()) {
         log.info("Stop called, nothing to do here(" + getName() + ")");
      }
   }


   @Override
   public RemoteCache withFlags(Flag... flags) {
      this.flagsMap.set(flags);
      return this;
   }

   private Flag[] flags() {
      Flag[] flags = this.flagsMap.get();
      this.flagsMap.remove();
      if (flags == null && forceReturnValue) {
         return FORCE_RETURN_VALUE;
      }
      return flags;
   }

   private byte[] obj2bytes(Object obj, boolean isKey) {
      return this.marshaller.marshallObject(obj, isKey);
   }

   private Object bytes2obj(byte[] bytes) {
      if (bytes == null) {
         return null;
      }
      return this.marshaller.readObject(bytes);
   }

   private VersionedValue<V> binary2VersionedValue(BinaryVersionedValue value) {
      if (value == null)
         return null;
      V valueObj = (V) bytes2obj(value.getValue());
      return new VersionedValueImpl<V>(value.getVersion(), valueObj);
   }

   private int toSeconds(long duration, TimeUnit timeUnit) {
      return (int) timeUnit.toSeconds(duration);
   }

   private void assertRemoteCacheManagerIsStarted() {
      if (!remoteCacheManager.isStarted()) {
         String message = "Cannot perform operations on a cache associated with an unstarted RemoteCacheManager. Use RemoteCacheManager.start before using the remote cache.";
         if (log.isInfoEnabled()) {
            log.info(message);
         }
         throw new RemoteCacheManagerNotStartedException(message);
      }
   }
}
