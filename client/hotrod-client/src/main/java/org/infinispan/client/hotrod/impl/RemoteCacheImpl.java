package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.Version;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.impl.async.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 * //todo - consider the return values
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> extends RemoteCacheSupport<K, V> {

   private static final Flag[] FORCE_RETURN_VALUE = {Flag.FORCE_RETURN_VALUE};

   private ThreadLocal<Flag[]> flagsMap = new ThreadLocal<Flag[]>();
   private HotrodOperations operations;
   private HotrodMarshaller marshaller;
   private String name;
   private RemoteCacheManager remoteCacheManager;
   private final ExecutorService executorService;
   private final boolean forceReturnValue;


   public RemoteCacheImpl(HotrodOperations operations, HotrodMarshaller marshaller, String name, RemoteCacheManager rcm, ExecutorService executorService, boolean forceReturnValue) {
      this.operations = operations;
      this.marshaller = marshaller;
      this.name = name;
      this.remoteCacheManager = rcm;
      this.executorService = executorService;
      this.forceReturnValue = forceReturnValue;
   }

   public RemoteCacheManager getRemoteCacheManager() {
      return remoteCacheManager;
   }

   @Override
   public boolean removeWithVersion(K key, long version) {
      VersionedOperationResponse response = operations.removeIfUnmodified(obj2bytes(key), version, flags());
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> removeWithVersionAsync(final K key, final long version) {
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
      VersionedOperationResponse response = operations.replaceIfUnmodified(obj2bytes(key), obj2bytes(newValue), lifespanSeconds, maxIdleTimeSeconds, version, flags());
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(final K key, final V newValue, final long version, final int lifespanSeconds, final int maxIdleSeconds) {
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
      BinaryVersionedValue value = operations.getWithVersion(obj2bytes(key), flags());
      return binary2VersionedValue(value);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         put(entry.getKey(), entry.getValue(), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(final Map<? extends K, ? extends V> data, final long lifespan, final TimeUnit lifespanUnit, final long maxIdle, final TimeUnit maxIdleUnit) {
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
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      byte[] result = operations.put(obj2bytes(key), obj2bytes(value), lifespanSecs, maxIdleSecs, flags());
      return (V) bytes2obj(result);
   }


   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      byte[] bytes = operations.putIfAbsent(obj2bytes(key), obj2bytes(value), lifespanSecs, maxIdleSecs, flags());
      return (V) bytes2obj(bytes);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      int lifespanSecs = toSeconds(lifespan, lifespanUnit);
      int maxIdleSecs = toSeconds(maxIdleTime, maxIdleTimeUnit);
      byte[] bytes = operations.replace(obj2bytes(key), obj2bytes(value), lifespanSecs, maxIdleSecs, flags());
      return (V) bytes2obj(bytes);
   }

   @Override
   public NotifyingFuture<V> putAsync(final K key, final V value, final long lifespan, final TimeUnit lifespanUnit, final long maxIdle, final TimeUnit maxIdleUnit) {
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
      return operations.containsKey(obj2bytes(key), flags());
   }

   @Override
   public V get(Object key) {
      byte[] bytes = operations.get(obj2bytes(key), flags());
      return (V) bytes2obj(bytes);
   }

   @Override
   public V remove(Object key) {
      byte[] existingValue = operations.remove(obj2bytes(key), flags());
      return (V) bytes2obj(existingValue);
   }

   @Override
   public void clear() {
      operations.clear(flags());
   }

   @Override
   public boolean ping() {
      return operations.ping();
   }

   @Override
   public void start() {
      // TODO: Customise this generated block
   }

   @Override
   public void stop() {
      // TODO: Customise this generated block
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

   private byte[] obj2bytes(Object obj) {
      return this.marshaller.marshallObject(obj);
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
      //todo make sure this can pe enveloped on an int
      return (int) timeUnit.toSeconds(duration);
   }
}
