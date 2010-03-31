package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.Version;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> extends RemoteCacheSupport<K,V> {

   private ThreadLocal<Flag[]> flagsMap = new ThreadLocal<Flag[]>();
   private HotrodOperations operations;
   private HotrodMarshaller marshaller;
   private String name;


   public RemoteCacheImpl(HotrodOperations operations, HotrodMarshaller marshaller, String name) {
      this.operations = operations;
      this.marshaller = marshaller;
      this.name = name;
   }

   @Override
   public boolean remove(K key, long version) {
      VersionedOperationResponse response = operations.removeIfUnmodified(obj2bytes(key), version, flags());
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, long version) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public boolean replace(K key, V newValue, long version, int lifespanSeconds, int maxIdleTimeSeconds) {
      VersionedOperationResponse response = operations.replaceIfUnmodified(obj2bytes(key), obj2bytes(newValue), lifespanSeconds, maxIdleTimeSeconds, version, flags());
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V newValue, long version, int lifespanSeconds, int maxIdleSeconds) {
      return null;  // TODO: Customise this generated block
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
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public ServerStatistics stats() {
      Map<String, Number> statsMap = operations.stats();
      ServerStatisticsImpl stats = new ServerStatisticsImpl();
      for (Map.Entry<String, Number> entry : statsMap.entrySet()) {
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
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public NotifyingFuture<V> removeAsync(Object key) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return null;  // TODO: Customise this generated block
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

   private int toSeconds(long durration, TimeUnit timeUnit) {
      //todo make sure this can pe enveloped on an int
      return (int) timeUnit.toSeconds(durration);
   }
}
