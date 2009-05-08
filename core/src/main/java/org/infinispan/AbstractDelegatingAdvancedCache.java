package org.infinispan;

import org.infinispan.batch.BatchContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.RpcManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Similar to {@link org.infinispan.AbstractDelegatingCache}, but for {@link AdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.AbstractDelegatingCache
 */
public class AbstractDelegatingAdvancedCache<K, V> extends AbstractDelegatingCache<K, V> implements AdvancedCache<K, V> {

   private AdvancedCache<K, V> cache;

   public AbstractDelegatingAdvancedCache(AdvancedCache<K, V> cache) {
      super(cache);
      this.cache = cache;
   }

   public void addInterceptor(CommandInterceptor i, int position) {
      cache.addInterceptor(i, position);
   }

   public void addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      cache.addInterceptorAfter(i, afterInterceptor);
   }

   public void addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      cache.addInterceptorBefore(i, beforeInterceptor);
   }

   public void removeInterceptor(int position) {
      cache.removeInterceptor(position);
   }

   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      cache.removeInterceptor(interceptorType);
   }

   public List<CommandInterceptor> getInterceptorChain() {
      return cache.getInterceptorChain();
   }

   public EvictionManager getEvictionManager() {
      return cache.getEvictionManager();
   }

   public ComponentRegistry getComponentRegistry() {
      return cache.getComponentRegistry();
   }

   public RpcManager getRpcManager() {
      return cache.getRpcManager();
   }

   public BatchContainer getBatchContainer() {
      return cache.getBatchContainer();
   }

   public InvocationContextContainer getInvocationContextContainer() {
      return cache.getInvocationContextContainer();
   }

   public DataContainer getDataContainer() {
      return cache.getDataContainer();
   }

   public void putForExternalRead(K key, V value, Flag... flags) {
      cache.putForExternalRead(key, value, flags);
   }

   public V put(K key, V value, Flag... flags) {
      return cache.put(key, value, flags);
   }

   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return cache.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   public V putIfAbsent(K key, V value, Flag... flags) {
      return cache.putIfAbsent(key, value, flags);
   }

   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return cache.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   public void putAll(Map<? extends K, ? extends V> map, Flag... flags) {
      cache.putAll(map, flags);
   }

   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      cache.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   public V remove(Object key, Flag... flags) {
      return cache.remove(key, flags);
   }

   public void clear(Flag... flags) {
      cache.clear(flags);
   }

   public V replace(K k, V v, Flag... flags) {
      return cache.replace(k, v, flags);
   }

   public V replace(K k, V oV, V nV, Flag... flags) {
      return cache.replace(k, oV, nV, flags);
   }

   public boolean replace(K k, V v, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      return cache.replace(k, v, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags);
   }

   public boolean replace(K k, V oV, V nV, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      return cache.replace(k, oV, nV, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags);
   }

   public Future<V> putAsync(K key, V value, Flag... flags) {
      return cache.putAsync(key, value, flags);
   }

   public Future<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return cache.putAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   public Future<V> putIfAbsentAsync(K key, V value, Flag... flags) {
      return cache.putIfAbsentAsync(key, value, flags);
   }

   public Future<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return cache.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   public Future<Void> putAllAsync(Map<? extends K, ? extends V> map, Flag... flags) {
      return cache.putAllAsync(map, flags);
   }

   public Future<Void> putAllAsync(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return cache.putAllAsync(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   public Future<V> removeAsync(Object key, Flag... flags) {
      return cache.removeAsync(key, flags);
   }

   public Future<Void> clearAsync(Flag... flags) {
      return cache.clearAsync(flags);
   }

   public Future<V> replaceAsync(K k, V v, Flag... flags) {
      return cache.replaceAsync(k, v, flags);
   }

   public Future<V> replaceAsync(K k, V oV, V nV, Flag... flags) {
      return cache.replaceAsync(k, oV, nV, flags);
   }

   public Future<Boolean> replaceAsync(K k, V v, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      return cache.replaceAsync(k, v, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags);
   }

   public Future<Boolean> replaceAsync(K k, V oV, V nV, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      return cache.replaceAsync(k, oV, nV, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags);
   }

   public boolean containsKey(Object key, Flag... flags) {
      return cache.containsKey(key, flags);
   }

   public V get(Object key, Flag... flags) {
      return cache.get(key, flags);
   }

   public Future<V> putAsync(K key, V value) {
      return cache.putAsync(key, value);
   }

   public Future<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putAsync(key, value, lifespan, unit);
   }

   public Future<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public Future<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return cache.putAllAsync(data);
   }

   public Future<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return cache.putAllAsync(data, lifespan, unit);
   }

   public Future<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public Future<Void> clearAsync() {
      return cache.clearAsync();
   }

   public Future<V> putIfAbsentAsync(K key, V value) {
      return cache.putIfAbsentAsync(key, value);
   }

   public Future<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putIfAbsentAsync(key, value, lifespan, unit);
   }

   public Future<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public Future<V> removeAsync(Object key) {
      return cache.removeAsync(key);
   }

   public Future<Boolean> removeAsync(Object key, Object value) {
      return cache.removeAsync(key, value);
   }

   public Future<V> replaceAsync(K key, V value) {
      return cache.replaceAsync(key, value);
   }

   public Future<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(key, value, lifespan, unit);
   }

   public Future<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public Future<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return cache.replaceAsync(key, oldValue, newValue);
   }

   public Future<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   public Future<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }
}
