package org.infinispan.security.impl;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.security.auth.Subject;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.LockedStream;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.SecureCache;
import org.infinispan.stats.Stats;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * SecureCacheImpl.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public final class SecureCacheImpl<K, V> implements SecureCache<K, V> {

   private final AuthorizationManager authzManager;
   private final AdvancedCache<K, V> delegate;
   private final Subject subject;
   private final AuthorizationPermission writePermission;

   public SecureCacheImpl(AdvancedCache<K, V> delegate) {
      this(delegate, delegate.getAuthorizationManager(), null);
   }

   private SecureCacheImpl(AdvancedCache<K, V> delegate, AuthorizationManager authzManager, Subject subject) {
      this.authzManager = authzManager;
      this.delegate = delegate;
      this.subject = subject;
      this.writePermission = authzManager.getWritePermission();
   }

   public AdvancedCache<K, V> getDelegate() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate;
   }

   @Override
   public AdvancedCache<K, V> withSubject(Subject subject) {
      if (this.subject == null) {
         return new SecureCacheImpl<>(delegate, authzManager, subject);
      } else {
         throw new IllegalArgumentException("Cannot set a Subject on a SecureCache more than once");
      }
   }

   @Override
   public boolean startBatch() {
      authzManager.checkPermission(subject, writePermission);
      return delegate.startBatch();
   }

   @Override
   public <C> CompletionStage<Void> addListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter,
         CacheEventConverter<? super K, ? super V, C> converter) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      return delegate.addListenerAsync(listener, filter, converter);
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      return delegate.addListenerAsync(listener);
   }

   @Override
   public <C> CompletionStage<Void> addFilteredListenerAsync(Object listener,
         CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
         Set<Class<? extends Annotation>> filterAnnotations) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      return delegate.addFilteredListenerAsync(listener, filter, converter, filterAnnotations);
   }

   @Override
   public <C> CompletionStage<Void> addStorageFormatFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, Set<Class<? extends Annotation>> filterAnnotations) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      return delegate.addStorageFormatFilteredListenerAsync(listener, filter, converter, filterAnnotations);
   }

   @Override
   public void shutdown() {
      authzManager.checkPermission(subject, AuthorizationPermission.LIFECYCLE);
      delegate.shutdown();
   }

   @Override
   public void start() {
      authzManager.checkPermission(subject, AuthorizationPermission.LIFECYCLE);
      delegate.start();
   }

   @Override
   public void stop() {
      authzManager.checkPermission(subject, AuthorizationPermission.LIFECYCLE);
      delegate.stop();
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAsync(key, value);
   }

   @Override
   public void endBatch(boolean successful) {
      authzManager.checkPermission(subject, writePermission);
      delegate.endBatch(successful);
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      return delegate.removeListenerAsync(listener);
   }

   @Deprecated
   @Override
   public Set<Object> getListeners() {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      return delegate.getListeners();
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAsync(key, value, lifespan, unit);
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      return new SecureCacheImpl<>(delegate.withFlags(flags), authzManager, subject);
   }

   @Override
   public AdvancedCache<K, V> withFlags(Collection<Flag> flags) {
      return new SecureCacheImpl<>(delegate.withFlags(flags), authzManager, subject);
   }

   @Override
   public AdvancedCache<K, V> noFlags() {
      return new SecureCacheImpl<>(delegate.noFlags(), authzManager, subject);
   }

   @Override
   public AdvancedCache<K, V> transform(Function<AdvancedCache<K, V>, ? extends AdvancedCache<K, V>> transformation) {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      AdvancedCache<K, V> newDelegate = delegate.transform(transformation);
      AdvancedCache<K, V> newInstance = newDelegate != delegate ? new SecureCacheImpl<>(newDelegate, authzManager, subject) : this;
      return transformation.apply(newInstance);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsent(key, value);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
                                        TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public String getName() {
      return delegate.getName();
   }

   @Override
   public String getVersion() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getVersion();
   }

   @Override
   public V put(K key, V value) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.put(key, value);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAllAsync(data);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.put(key, value, lifespan, unit);
   }

   @Override
   public boolean remove(Object key, Object value) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.remove(key, value);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAllAsync(data, lifespan, unit);
   }

   /**
    * @deprecated Since 10.0, will be removed without a replacement
    */
   @Deprecated
   @Override
   public AsyncInterceptorChain getAsyncInterceptorChain() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getAsyncInterceptorChain();
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsent(key, value, lifespan, unit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit,
                                              long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      delegate.putAll(map, lifespan, unit);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replace(key, oldValue, newValue);
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_WRITE);
      return delegate.clearAsync();
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replace(key, value, lifespan, unit);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsentAsync(key, value);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replace(key, oldValue, value, lifespan, unit);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsentAsync(key, value, lifespan, unit);
   }

   @Override
   public V replace(K key, V value) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replace(key, value);
   }

   @Override
   public EvictionManager getEvictionManager() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getEvictionManager();
   }

   @Override
   public ExpirationManager<K, V> getExpirationManager() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getExpirationManager();
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      authzManager.checkPermission(subject, writePermission);
      delegate.putForExternalRead(key, value);
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      delegate.putForExternalRead(key, value);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.compute(key, remappingFunction);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.merge(key, value, remappingFunction);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.merge(key, value, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.merge(key, value, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.merge(key, value, remappingFunction, metadata);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.compute(key, remappingFunction, metadata);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.compute(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.compute(key, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfPresent(key, remappingFunction);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfPresent(key, remappingFunction, metadata);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfPresent(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfPresent(key, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfAbsent(key, mappingFunction);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfAbsent(key, mappingFunction, metadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfAbsent(key, mappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfAbsent(key, mappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeAsync(key, remappingFunction, metadata);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfPresentAsync(key, remappingFunction, metadata);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfAbsentAsync(key, mappingFunction, metadata);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.mergeAsync(key, value, remappingFunction, metadata);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeAsync(key, remappingFunction);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeAsync(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfAbsentAsync(key, mappingFunction);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfPresentAsync(key, remappingFunction);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.mergeAsync(key, value, remappingFunction);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      delegate.putForExternalRead(key, value);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, writePermission);
      delegate.putForExternalRead(key, value);
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getComponentRegistry();
   }

   @Override
   public DistributionManager getDistributionManager() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getDistributionManager();
   }

   @Override
   public AuthorizationManager getAuthorizationManager() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return new AuthorizationManager() {
         @Override
         public void checkPermission(AuthorizationPermission permission) {
            authzManager.checkPermission(subject, permission);
         }

         @Override
         public void checkPermission(Subject subject, AuthorizationPermission permission) {
            authzManager.checkPermission(subject, permission);
         }

         @Override
         public void checkPermission(AuthorizationPermission permission, String role) {
            authzManager.checkPermission(subject, permission, role);
         }

         @Override
         public void checkPermission(Subject subject, AuthorizationPermission permission, String role) {
            authzManager.checkPermission(subject, permission, role);
         }

         @Override
         public AuthorizationPermission getWritePermission() {
            return authzManager.getWritePermission();
         }
      };
   }

   @Override
   public AdvancedCache<K, V> lockAs(Object lockOwner) {
      return new SecureCacheImpl<>(delegate.lockAs(lockOwner));
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
                                                TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public boolean isEmpty() {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.isEmpty();
   }

   @Override
   public boolean lock(K... keys) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.lock(keys);
   }

   @Override
   public boolean containsKey(Object key) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.containsKey(key);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.lock(keys);
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.removeAsync(key);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> removeAsyncEntry(Object key) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.removeAsyncEntry(key);
   }

   @Override
   public boolean containsValue(Object value) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.containsValue(value);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
                      TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      delegate.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.removeAsync(key, value);
   }

   @Override
   public void evict(K key) {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      delegate.evict(key);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsync(key, value);
   }

   @Override
   public RpcManager getRpcManager() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getRpcManager();
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V get(Object key) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.get(key);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsync(key, value, lifespan, unit);
   }

   @Override
   public BatchContainer getBatchContainer() {
      authzManager.checkPermission(subject, writePermission);
      return delegate.getBatchContainer();
   }

   @Override
   public Configuration getCacheConfiguration() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getCacheConfiguration();
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getCacheManager();
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
   public AvailabilityMode getAvailability() {
      return delegate.getAvailability();
   }

   @Override
   public void setAvailability(AvailabilityMode availabilityMode) {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      delegate.setAvailability(availabilityMode);
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.cacheEntrySet();
   }

   @Override
   public LockedStream<K, V> lockedStream() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_WRITE);
      return delegate.lockedStream();
   }

   @Override
   public CompletableFuture<Boolean> removeLifespanExpired(K key, V value, Long lifespan) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.removeLifespanExpired(key, value, lifespan);
   }

   @Override
   public CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.removeMaxIdleExpired(key, value);
   }

   @Override
   public AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> encoderClass) {
      return new SecureCacheImpl<>(delegate.withEncoding(encoderClass), authzManager, subject);
   }

   @Override
   public AdvancedCache<?, ?> withKeyEncoding(Class<? extends Encoder> encoder) {
      return new SecureCacheImpl<>(delegate.withKeyEncoding(encoder), authzManager, subject);
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapperClass) {
      return new SecureCacheImpl<>(delegate.withWrapping(wrapperClass), authzManager, subject);
   }

   @Override
   public AdvancedCache<?, ?> withMediaType(String keyMediaType, String valueMediaType) {
      return new SecureCacheImpl<>(delegate.withMediaType(keyMediaType, valueMediaType), authzManager, subject);
   }

   @Override
   public <K1, V1> AdvancedCache<K1, V1> withMediaType(MediaType keyMediaType, MediaType valueMediaType) {
      return new SecureCacheImpl<>(delegate.withMediaType(keyMediaType, valueMediaType), authzManager, subject);
   }

   @Override
   public AdvancedCache<K, V> withStorageMediaType() {
      return new SecureCacheImpl<>(delegate.withStorageMediaType(), authzManager, subject);
   }

   @Override
   public AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> keyEncoderClass,
                                           Class<? extends Encoder> valueEncoderClass) {
      return new SecureCacheImpl<>(delegate.withEncoding(keyEncoderClass, valueEncoderClass), authzManager, subject);
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapperClass,
                                           Class<? extends Wrapper> valueWrapperClass) {
      return new SecureCacheImpl<>(delegate.withWrapping(keyWrapperClass, valueWrapperClass), authzManager, subject);
   }

   @Override
   public DataConversion getKeyDataConversion() {
      return delegate.getKeyDataConversion();
   }

   @Override
   public DataConversion getValueDataConversion() {
      return delegate.getValueDataConversion();
   }

   @Override
   public int size() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.size();
   }

   @Override
   public CompletableFuture<Long> sizeAsync() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.sizeAsync();
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
                          TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public DataContainer<K, V> getDataContainer() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getDataContainer();
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
                                            TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public TransactionManager getTransactionManager() {
      return delegate.getTransactionManager();
   }

   @Override
   public CacheSet<K> keySet() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.keySet();
   }

   @Override
   public V remove(Object key) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.remove(key);
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.getAll(keys);
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.getAllAsync(keys);
   }

   @Override
   public LockManager getLockManager() {
      authzManager.checkPermission(subject, writePermission);
      return delegate.getLockManager();
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsync(key, oldValue, newValue);
   }

   @Override
   public Stats getStats() {
      authzManager.checkPermission(subject, AuthorizationPermission.MONITOR);
      return delegate.getStats();
   }

   @Override
   public XAResource getXAResource() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getXAResource();
   }

   @Override
   public ClassLoader getClassLoader() {
      return delegate.getClassLoader();
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   @Override
   public CacheCollection<V> values() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.values();
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return this;
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit,
                                                  long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.entrySet();
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.getAsync(key);
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.put(key, value, metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      delegate.putAll(m, metadata);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAllAsync(map, metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      authzManager.checkPermission(subject, writePermission);
      delegate.putAll(m);
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replace(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsync(key, value, metadata);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> replaceAsyncEntry(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsyncEntry(key, value, metadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replace(key, oldValue, newValue, metadata);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsync(key, oldValue, newValue, metadata);
   }

   @Override
   public void clear() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_WRITE);
      delegate.clear();
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsent(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsentAsync(key, value, metadata);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putIfAbsentAsyncEntry(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsentAsyncEntry(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAsync(key, value, metadata);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putAsyncEntry(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAsyncEntry(key, value, metadata);
   }
   @Override
   public CacheEntry<K, V> getCacheEntry(Object key) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.getCacheEntry(key);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> getCacheEntryAsync(Object key) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.getCacheEntryAsync(key);
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.getAllCacheEntries(keys);
   }

   @Override
   public Map<K, V> getAndPutAll(Map<? extends K, ? extends V> map) {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_WRITE);
      return delegate.getAndPutAll(map);
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.getGroup(groupName);
   }

   @Override
   public void removeGroup(String groupName) {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_WRITE);
      delegate.removeGroup(groupName);
   }

   @Override
   public boolean equals(Object o) {
      return delegate.equals(o);
   }

   @Override
   public int hashCode() {
      return delegate.hashCode();
   }

   @Override
   public String toString() {
      return "Secure " + delegate;
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, int segment, boolean touchEvenIfExpired) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.touch(key, segment, touchEvenIfExpired);
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, boolean touchEvenIfExpired) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.touch(key, touchEvenIfExpired);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> putAsyncEntry(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putAsyncEntry(key, value, metadata);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> replaceAsyncEntry(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.replaceAsyncEntry(key, value, metadata);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> removeAsyncEntry(K key) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.removeAsyncEntry(key);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> putIfAbsentAsyncEntry(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, writePermission);
      return delegate.putIfAbsentAsyncEntry(key, value, metadata);
   }
}
