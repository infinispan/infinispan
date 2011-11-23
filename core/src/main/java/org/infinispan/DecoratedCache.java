/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.util.concurrent.NotifyingFuture;
import org.infinispan.context.Flag;

/**
 * A decorator to a cache, which can be built with a specific {@link ClassLoader} and a set of {@link Flag}s.  This
 * {@link ClassLoader} and set of {@link Flag}s will be applied to all cache invocations made via this decorator.
 * <p/>
 * In addition to cleaner and more readable code, this approach offers a performance benefit to using {@link
 * AdvancedCache#with(ClassLoader)} or {@link AdvancedCache#withFlags(org.infinispan.context.Flag...)} APIs, thanks to
 * internal optimizations that can be made when the {@link ClassLoader} and {@link Flag} set is unchanging.
 *
 * @author Manik Surtani
 * @see AdvancedCache#with(ClassLoader)
 * @see AdvancedCache#withFlags(org.infinispan.context.Flag...)
 * @since 5.1
 */
public class DecoratedCache<K, V> extends AbstractDelegatingAdvancedCache<K, V> {

   private EnumSet<Flag> flags;
   private ClassLoader classLoader;
   private final CacheImpl<K, V> cacheImplementation;


   public DecoratedCache(AdvancedCache<K, V> delegate, ClassLoader classLoader) {
      this(delegate, classLoader, null);
   }

   public DecoratedCache(AdvancedCache<K, V> delegate, Flag... flags) {
      this(delegate, null, flags);
   }

   public DecoratedCache(AdvancedCache<K, V> delegate, ClassLoader classLoader, Flag... flags) {
      super(delegate);
      if (flags == null || flags.length == 0)
         this.flags = null;
      else {
         this.flags = EnumSet.noneOf(Flag.class);
         this.flags.addAll(Arrays.asList(flags));
      }
      this.classLoader = classLoader;

      if (flags == null && classLoader == null)
         throw new IllegalArgumentException("There is no point in using a DecoratedCache if neither a ClassLoader nor any Flags are set.");

      // Yuk
      cacheImplementation = (CacheImpl<K, V>) delegate;
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      if (classLoader == null) throw new NullPointerException("ClassLoader passed in cannot be null!");
      this.classLoader = classLoader;
      return this;
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      if (flags == null) throw new NullPointerException("Flags cannot be null!");
      if (this.flags == null) this.flags = EnumSet.noneOf(Flag.class);
      this.flags.addAll(Arrays.asList(flags));

      return this;
   }

   @Override
   public ClassLoader getClassLoader() {
      if (this.classLoader == null) {
         return cacheImplementation.getClassLoader();
      }
      else {
         return this.classLoader;
      }
   }

   @Override
   public void stop() {
      cacheImplementation.stop(flags, classLoader);
   }

   @Override
   public boolean lock(K... keys) {
      return cacheImplementation.lock(Arrays.asList(keys), flags, classLoader);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return cacheImplementation.lock(keys, flags, classLoader);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      cacheImplementation.putForExternalRead(key, value, flags, classLoader);
   }

   @Override
   public void evict(K key) {
      cacheImplementation.evict(key, flags, classLoader);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return cacheImplementation.put(key, value, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return cacheImplementation.putIfAbsent(key, value, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      cacheImplementation.putAll(map, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      return cacheImplementation.replace(key, value, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return cacheImplementation.replace(key, oldValue, value, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cacheImplementation.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cacheImplementation.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      cacheImplementation.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cacheImplementation.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cacheImplementation.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value) {
      return cacheImplementation.putAsync(key, value, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cacheImplementation.putAsync(key, value, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cacheImplementation.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return cacheImplementation.putAllAsync(data, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return cacheImplementation.putAllAsync(data, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cacheImplementation.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      return cacheImplementation.clearAsync(flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      return cacheImplementation.putIfAbsentAsync(key, value, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cacheImplementation.putIfAbsentAsync(key, value, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cacheImplementation.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> removeAsync(Object key) {
      return cacheImplementation.removeAsync(key, flags, classLoader);
   }

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      return cacheImplementation.removeAsync(key, value, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value) {
      return cacheImplementation.replaceAsync(key, value, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cacheImplementation.replaceAsync(key, value, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cacheImplementation.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return cacheImplementation.replaceAsync(key, oldValue, newValue, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return cacheImplementation.replaceAsync(key, oldValue, newValue, lifespan, unit, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cacheImplementation.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   @Override
   public NotifyingFuture<V> getAsync(K key) {
      return cacheImplementation.getAsync(key, flags, classLoader);
   }

   @Override
   public int size() {
      return cacheImplementation.size(flags, classLoader);
   }

   @Override
   public boolean isEmpty() {
      return cacheImplementation.isEmpty(flags, classLoader);
   }

   @Override
   public boolean containsKey(Object key) {
      return cacheImplementation.containsKey(key, flags, classLoader);
   }

   @Override
   public V get(Object key) {
      return cacheImplementation.get(key, flags, classLoader);
   }

   @Override
   public V put(K key, V value) {
      return cacheImplementation.put(key, value, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public V remove(Object key) {
      return cacheImplementation.remove(key, flags, classLoader);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      cacheImplementation.putAll(m, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public void clear() {
      cacheImplementation.clear(flags, classLoader);
   }

   @Override
   public Set<K> keySet() {
      return cacheImplementation.keySet(flags, classLoader);
   }

   @Override
   public Collection<V> values() {
      return cacheImplementation.values(flags, classLoader);
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return cacheImplementation.entrySet(flags, classLoader);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return cacheImplementation.putIfAbsent(key, value, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return cacheImplementation.remove(key, value, flags, classLoader);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return cacheImplementation.replace(key, oldValue, newValue, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }

   @Override
   public V replace(K key, V value) {
      return cacheImplementation.replace(key, value, cacheImplementation.getDefaultLifespan(), MILLISECONDS, cacheImplementation.getDefaultMaxIdleTime(), MILLISECONDS, flags, classLoader);
   }
}
