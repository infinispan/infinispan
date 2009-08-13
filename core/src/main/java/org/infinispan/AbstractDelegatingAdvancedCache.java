package org.infinispan;

import org.infinispan.batch.BatchContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Similar to {@link org.infinispan.AbstractDelegatingCache}, but for {@link AdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.AbstractDelegatingCache
 */
public abstract class AbstractDelegatingAdvancedCache<K, V> extends AbstractDelegatingCache<K, V> implements AdvancedCache<K, V> {

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

   public boolean replace(K k, V oV, V nV, Flag... flags) {
      return cache.replace(k, oV, nV, flags);
   }

   public V replace(K k, V v, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      return cache.replace(k, v, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags);
   }

   public boolean replace(K k, V oV, V nV, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      return cache.replace(k, oV, nV, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags);
   }

   public NotifyingFuture<V> putAsync(K key, V value, Flag... flags) {
      return cache.putAsync(key, value, flags);
   }

   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return cache.putAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, Flag... flags) {
      return cache.putIfAbsentAsync(key, value, flags);
   }

   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return cache.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, Flag... flags) {
      return cache.putAllAsync(map, flags);
   }

   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return cache.putAllAsync(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags);
   }

   public NotifyingFuture<V> removeAsync(Object key, Flag... flags) {
      return cache.removeAsync(key, flags);
   }

   public NotifyingFuture<Void> clearAsync(Flag... flags) {
      return cache.clearAsync(flags);
   }

   public NotifyingFuture<V> replaceAsync(K k, V v, Flag... flags) {
      return cache.replaceAsync(k, v, flags);
   }

   public NotifyingFuture<Boolean> replaceAsync(K k, V oV, V nV, Flag... flags) {
      return cache.replaceAsync(k, oV, nV, flags);
   }

   public NotifyingFuture<V> replaceAsync(K k, V v, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      return cache.replaceAsync(k, v, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags);
   }

   public NotifyingFuture<Boolean> replaceAsync(K k, V oV, V nV, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags) {
      return cache.replaceAsync(k, oV, nV, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags);
   }

   public boolean containsKey(Object key, Flag... flags) {
      return cache.containsKey(key, flags);
   }

   public V get(Object key, Flag... flags) {
      return cache.get(key, flags);
   }

   public void lock(K key) {
      cache.lock(key);
   }

   public void lock(Collection<? extends K> keys) {
      cache.lock(keys);
   }
}
