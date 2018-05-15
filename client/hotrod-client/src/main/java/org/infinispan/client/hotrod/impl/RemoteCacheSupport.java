package org.infinispan.client.hotrod.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.infinispan.client.hotrod.RemoteCache;

/**
 * Purpose: keep all delegating and unsupported methods in one place -> readability.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class RemoteCacheSupport<K,V> implements RemoteCache<K,V> {
   protected long defaultLifespan;
   protected long defaultMaxIdleTime;

   protected RemoteCacheSupport() {
      this(0, 0);
   }

   protected RemoteCacheSupport(long defaultLifespan, long defaultMaxIdleTime) {
      this.defaultLifespan = defaultLifespan;
      this.defaultMaxIdleTime = defaultMaxIdleTime;
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      putAll(map, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return putAllAsync(data, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return putAllAsync(data, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map) {
      putAll(map, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsent(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return replace(key, oldValue, newValue, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V replace(K key, V value) {
      return replace(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V replace(K key, V value, long lifespan, TimeUnit unit) {
      return replace(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V put(K key, V value) {
      return put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   };

   /**
    * This is intentionally a non-public method meant as an integration point for bytecode manipulation. Don't remove or
    * alter the signature even if it might look like unreachable code. Implementors should perform a put operation but
    * optimizing it as return values are not required.
    *
    * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
    * @since 5.0
    */
   protected abstract void set(K key, V value);

   @Override
   public final CompletableFuture<V> putAsync(K key, V value) {
      return putAsync(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putAsync(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version) {
      return replaceWithVersionAsync(key, newValue, version, 0);
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds) {
      return replaceWithVersionAsync(key, newValue, version, lifespanSeconds, 0);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version) {
      return replaceWithVersion(key, newValue, version, 0);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds) {
      return replaceWithVersion(key, newValue, version, lifespanSeconds, 0);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return replaceAsync(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return put(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }


   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return putIfAbsentAsync(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit) {
      return putIfAbsentAsync(key, value, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return replaceAsync(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return replace(key, oldValue, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return merge(key, value, remappingFunction, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan,
                  TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan,
                  TimeUnit lifespanUnit) {
      return merge(key, value, remappingFunction, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }
}
