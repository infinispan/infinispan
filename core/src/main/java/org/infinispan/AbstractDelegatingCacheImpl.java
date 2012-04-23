/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.stats.Stats;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.locks.LockManager;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Similar to {@link AbstractDelegatingAdvancedCache}, this adapter can only delegate to a {@link CacheImpl} instance.
 *
 * @author Manik Surtani
 * @see AbstractDelegatingCache
 * @see AbstractDelegatingAdvancedCache
 */
public abstract class AbstractDelegatingCacheImpl<K, V> extends AbstractDelegatingCache<K, V> implements AdvancedCache<K, V> {

   protected final CacheImpl<K, V> cache;

   public AbstractDelegatingCacheImpl(AdvancedCache<K, V> cache) {
      super(cache);
      if (cache instanceof CacheImpl)
         this.cache = (CacheImpl<K, V>) cache;
      else
         throw new IllegalArgumentException("This adapter only works with CacheImpl, not with " + cache.getClass().getSimpleName());
   }

   @Override
   public void addInterceptor(CommandInterceptor i, int position) {
      cache.addInterceptor(i, position);
   }

   @Override
   public boolean addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      return cache.addInterceptorAfter(i, afterInterceptor);
   }

   @Override
   public boolean addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      return cache.addInterceptorBefore(i, beforeInterceptor);
   }

   @Override
   public void removeInterceptor(int position) {
      cache.removeInterceptor(position);
   }

   @Override
   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      cache.removeInterceptor(interceptorType);
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      //We need to override the super implementation which returns to the decorated cache;
      //otherwise the current operation breaks out of the selected ClassLoader.
      return this;
   }

   @Override
   public List<CommandInterceptor> getInterceptorChain() {
      return cache.getInterceptorChain();
   }

   @Override
   public EvictionManager getEvictionManager() {
      return cache.getEvictionManager();
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      return cache.getComponentRegistry();
   }

   @Override
   public DistributionManager getDistributionManager() {
      return cache.getDistributionManager();
   }

   @Override
   public RpcManager getRpcManager() {
      return cache.getRpcManager();
   }

   @Override
   public BatchContainer getBatchContainer() {
      return cache.getBatchContainer();
   }

   @Override
   public InvocationContextContainer getInvocationContextContainer() {
      return cache.getInvocationContextContainer();
   }

   @Override
   public DataContainer getDataContainer() {
      return cache.getDataContainer();
   }

   @Override
   public TransactionManager getTransactionManager() {
      return cache.getTransactionManager();
   }

   @Override
   public LockManager getLockManager() {
      return cache.getLockManager();
   }

   @Override
   public XAResource getXAResource() {
      return cache.getXAResource();
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      cache.withFlags(flags);
      return this;
   }

   @Override
   public boolean lock(K... key) {
      return cache.lock(key);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return cache.lock(keys);
   }

   @Override
   public void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire){
      cache.applyDelta(deltaAwareValueKey, delta, locksToAcquire);
   }

   @Override
   public Stats getStats() {
       return cache.getStats();
   }

   @Override
   public ClassLoader getClassLoader() {
      return cache.getClassLoader();
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return cache.with(classLoader);
   }

   // ******************************************************************************************
   // Expose some package-protected methods on CacheImpl as protected for subclasses to use
   // ******************************************************************************************

   protected final void putForExternalRead(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      cache.putForExternalRead(key, value, flags, classLoader);
   }

   protected void evict(K key, EnumSet<Flag> flags, ClassLoader classLoader) {
      cache.evict(key, flags, classLoader);
   }

   protected V put(K key, V value, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.put(key, value, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected V putIfAbsent(K key, V value, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putIfAbsent(key, value, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      cache.putAll(map, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected V replace(K key, V value, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replace(key, value, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replace(key, oldValue, value, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   protected V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   protected void putAll(Map<? extends K,? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      cache.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   protected V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   protected boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags, classLoader);
   }

   protected NotifyingFuture<V> putAsync(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putAsync(key, value, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putAsync(key, value, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   protected NotifyingFuture<Void> putAllAsync(Map<? extends K,? extends V> data, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putAllAsync(data, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<Void> putAllAsync(Map<? extends K,? extends V> data, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putAllAsync(data, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<Void> putAllAsync(Map<? extends K,? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   protected NotifyingFuture<Void> clearAsync(EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.clearAsync(flags, classLoader);
   }

   protected NotifyingFuture<V> putIfAbsentAsync(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putIfAbsentAsync(key, value, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putIfAbsentAsync(key, value, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   protected NotifyingFuture<V> removeAsync(Object key, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.removeAsync(key, flags, classLoader);
   }

   protected NotifyingFuture<Boolean> removeAsync(Object key, Object value, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.removeAsync(key, value, flags, classLoader);
   }

   protected NotifyingFuture<V> replaceAsync(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replaceAsync(key, value, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replaceAsync(key, value, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   protected NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replaceAsync(key, oldValue, newValue, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, unit, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit, flags, classLoader);
   }

   protected int size(EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.size(flags, classLoader);
   }

   protected boolean isEmpty(EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.isEmpty(flags, classLoader);
   }

   protected boolean containsKey(Object key, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.containsKey(key, flags, classLoader);
   }

   protected V get(Object key, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.get(key, flags, classLoader);
   }

   protected V put(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.put(key, value, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected V remove(Object key, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.remove(key, flags, classLoader);
   }

   protected void putAll(Map<? extends K,? extends V> m, EnumSet<Flag> flags, ClassLoader classLoader) {
      cache.putAll(m, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected void clear(EnumSet<Flag> flags, ClassLoader classLoader) {
      cache.clear(flags, classLoader);
   }

   protected Set<K> keySet(EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.keySet(flags, classLoader);
   }

   protected Collection<V> values(EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.values(flags, classLoader);
   }

   protected Set<Entry<K, V>> entrySet(EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.entrySet(flags, classLoader);
   }

   protected V putIfAbsent(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.putIfAbsent(key, value, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected boolean remove(Object key, Object value, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.remove(key, value, flags, classLoader);
   }

   protected boolean replace(K key, V oldValue, V newValue, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replace(key, oldValue, newValue, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected V replace(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.replace(key, value, cache.defaultLifespan, MILLISECONDS, cache.defaultMaxIdleTime, MILLISECONDS, flags, classLoader);
   }

   protected NotifyingFuture<V> getAsync(K key, EnumSet<Flag> flags, ClassLoader classLoader) {
      return cache.getAsync(key, flags, classLoader);
   }
}
