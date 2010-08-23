package org.infinispan;

import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class CacheSupport<K,V> implements Cache<K,V> {
   protected long defaultLifespan;
   protected long defaultMaxIdleTime;

   protected CacheSupport() {
      this(0,0);
   }

   protected CacheSupport(long defaultLifespan, long defaultMaxIdleTime) {
      this.defaultLifespan = defaultLifespan;
      this.defaultMaxIdleTime = defaultMaxIdleTime;
   }

   public final V put(K key, V value) {
      return put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final void putAll(Map<? extends K, ? extends V> map) {
      putAll(map, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final NotifyingFuture<V> putAsync(K key, V value) {
      return putAsync(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final V putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putAsync(key, value, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return putAllAsync(data, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return putAllAsync(data, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      return putIfAbsentAsync(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsentAsync(key, value, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return replaceAsync(key, value, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return replaceAsync(key, oldValue, newValue, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final NotifyingFuture<V> replaceAsync(K key, V value) {
      return replaceAsync(key, value, MILLISECONDS.toMillis(defaultLifespan), MILLISECONDS, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return replaceAsync(key, oldValue, newValue, lifespan, unit, MILLISECONDS.toMillis(defaultMaxIdleTime), MILLISECONDS);
   }

   public final V put(K key, V value, long lifespan, TimeUnit unit) {
      return put(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsent(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      putAll(map, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public final V replace(K key, V value, long lifespan, TimeUnit unit) {
      return replace(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   public final boolean replace(K key, V oldValue, V newValue) {
      return replace(key, oldValue, newValue, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final V replace(K key, V value) {
      return replace(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return replace(key, oldValue, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }
}
