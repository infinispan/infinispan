 package org.infinispan.security.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Converter;
import org.infinispan.notifications.KeyFilter;
import org.infinispan.notifications.KeyValueFilter;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.SecureCache;
import org.infinispan.stats.Stats;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * SecureCacheImpl.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public final class SecureCacheImpl<K, V> implements SecureCache<K, V> {

   private final org.infinispan.security.AuthorizationManager authzManager;
   private final AdvancedCache<K, V> delegate;

   public SecureCacheImpl(AdvancedCache<K, V> delegate) {
      this.authzManager = delegate.getAuthorizationManager();
      this.delegate = delegate;
   }

   @Override
   public boolean startBatch() {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.startBatch();
   }

   @Override
   public <K, V, C> void addListener(Object listener, KeyValueFilter<K, V> filter, Converter<K, V, C> converter) {
      authzManager.checkPermission(AuthorizationPermission.LISTEN);
      delegate.addListener(listener, filter, converter);
   }

   @Override
   public void addListener(Object listener, KeyFilter filter) {
      authzManager.checkPermission(AuthorizationPermission.LISTEN);
      delegate.addListener(listener, filter);
   }

   @Override
   public void addListener(Object listener) {
      authzManager.checkPermission(AuthorizationPermission.LISTEN);
      delegate.addListener(listener);
   }

   @Override
   public void start() {
      authzManager.checkPermission(AuthorizationPermission.LIFECYCLE);
      delegate.start();
   }

   @Override
   public void stop() {
      authzManager.checkPermission(AuthorizationPermission.LIFECYCLE);
      delegate.stop();
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putAsync(key, value);
   }

   @Override
   public void endBatch(boolean successful) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      delegate.endBatch(successful);
   }

   @Override
   public void removeListener(Object listener) {
      authzManager.checkPermission(AuthorizationPermission.LISTEN);
      delegate.removeListener(listener);
   }

   @Override
   public Set<Object> getListeners() {
      authzManager.checkPermission(AuthorizationPermission.LISTEN);
      return delegate.getListeners();
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putAsync(key, value, lifespan, unit);
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      return new SecureCacheImpl(delegate.withFlags(flags));
   }

   @Override
   public V putIfAbsent(K key, V value) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putIfAbsent(key, value);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
         TimeUnit maxIdleUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public String getName() {
      return delegate.getName();
   }

   @Override
   public String getVersion() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getVersion();
   }

   @Override
   public V put(K key, V value) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.put(key, value);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putAllAsync(data);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.put(key, value, lifespan, unit);
   }

   @Override
   public boolean remove(Object key, Object value) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.remove(key, value);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putAllAsync(data, lifespan, unit);
   }

   @Override
   public void addInterceptor(CommandInterceptor i, int position) {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      delegate.addInterceptor(i, position);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putIfAbsent(key, value, lifespan, unit);
   }

   @Override
   public boolean addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.addInterceptorAfter(i, afterInterceptor);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit,
         long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      delegate.putAll(map, lifespan, unit);
   }

   @Override
   public boolean addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.addInterceptorBefore(i, beforeInterceptor);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replace(key, oldValue, newValue);
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      authzManager.checkPermission(AuthorizationPermission.BULK_WRITE);
      return delegate.clearAsync();
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replace(key, value, lifespan, unit);
   }

   @Override
   public void removeInterceptor(int position) {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      delegate.removeInterceptor(position);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putIfAbsentAsync(key, value);
   }

   @Override
   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      delegate.removeInterceptor(interceptorType);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replace(key, oldValue, value, lifespan, unit);
   }

   @Override
   public List<CommandInterceptor> getInterceptorChain() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getInterceptorChain();
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putIfAbsentAsync(key, value, lifespan, unit);
   }

   @Override
   public V replace(K key, V value) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replace(key, value);
   }

   @Override
   public EvictionManager getEvictionManager() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getEvictionManager();
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      delegate.putForExternalRead(key, value);
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getComponentRegistry();
   }

   @Override
   public DistributionManager getDistributionManager() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getDistributionManager();
   }

   @Override
   public AuthorizationManager getAuthorizationManager() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getAuthorizationManager();
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
         TimeUnit maxIdleUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public boolean isEmpty() {
      authzManager.checkPermission(AuthorizationPermission.READ);
      return delegate.isEmpty();
   }

   @Override
   public boolean lock(K... keys) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.lock(keys);
   }

   @Override
   public boolean containsKey(Object key) {
      authzManager.checkPermission(AuthorizationPermission.READ);
      return delegate.containsKey(key);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.lock(keys);
   }

   @Override
   public NotifyingFuture<V> removeAsync(Object key) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.removeAsync(key);
   }

   @Override
   public boolean containsValue(Object value) {
      authzManager.checkPermission(AuthorizationPermission.READ);
      return delegate.containsValue(value);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      delegate.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.removeAsync(key, value);
   }

   @Override
   public void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      delegate.applyDelta(deltaAwareValueKey, delta, locksToAcquire);
   }

   @Override
   public void evict(K key) {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      delegate.evict(key);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, value);
   }

   @Override
   public RpcManager getRpcManager() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getRpcManager();
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V get(Object key) {
      authzManager.checkPermission(AuthorizationPermission.READ);
      return delegate.get(key);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, value, lifespan, unit);
   }

   @Override
   public BatchContainer getBatchContainer() {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.getBatchContainer();
   }

   @Override
   public Configuration getCacheConfiguration() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getCacheConfiguration();
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getCacheManager();
   }

   @Override
   public InvocationContextContainer getInvocationContextContainer() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getInvocationContextContainer();
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      return this;
   }

   @Override
   public ComponentStatus getStatus() {
      return delegate.getStatus();
   }

   @Override
   public int size() {
      authzManager.checkPermission(AuthorizationPermission.BULK_READ);
      return delegate.size();
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public DataContainer getDataContainer() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getDataContainer();
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
         TimeUnit maxIdleUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public TransactionManager getTransactionManager() {
      return delegate.getTransactionManager();
   }

   @Override
   public Set<K> keySet() {
      authzManager.checkPermission(AuthorizationPermission.BULK_READ);
      return delegate.keySet();
   }

   @Override
   public V remove(Object key) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.remove(key);
   }

   @Override
   public LockManager getLockManager() {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.getLockManager();
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, oldValue, newValue);
   }

   @Override
   public Stats getStats() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getStats();
   }

   @Override
   public XAResource getXAResource() {
      authzManager.checkPermission(AuthorizationPermission.ADMIN);
      return delegate.getXAResource();
   }

   @Override
   public ClassLoader getClassLoader() {
      return delegate.getClassLoader();
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   @Override
   public Collection<V> values() {
      authzManager.checkPermission(AuthorizationPermission.BULK_READ);
      return delegate.values();
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return new SecureCacheImpl(delegate.with(classLoader));
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit,
         long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public Set<java.util.Map.Entry<K, V>> entrySet() {
      authzManager.checkPermission(AuthorizationPermission.BULK_READ);
      return delegate.entrySet();
   }

   @Override
   public NotifyingFuture<V> getAsync(K key) {
      authzManager.checkPermission(AuthorizationPermission.READ);
      return delegate.getAsync(key);
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.put(key, value, metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      delegate.putAll(m);
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replace(key, value, metadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue, Metadata metadata) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.replace(key, oldValue, newValue, metadata);
   }

   @Override
   public void clear() {
      authzManager.checkPermission(AuthorizationPermission.BULK_WRITE);
      delegate.clear();
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putIfAbsent(key, value, metadata);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, Metadata metadata) {
      authzManager.checkPermission(AuthorizationPermission.WRITE);
      return delegate.putAsync(key, value, metadata);
   }

   @Override
   public CacheEntry getCacheEntry(K key) {
      authzManager.checkPermission(AuthorizationPermission.READ);
      return delegate.getCacheEntry(key);
   }

   @Override
   public boolean equals(Object o) {
      return delegate.equals(o);
   }

   @Override
   public int hashCode() {
      return delegate.hashCode();
   }
}
