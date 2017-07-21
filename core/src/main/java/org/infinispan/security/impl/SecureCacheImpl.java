package org.infinispan.security.impl;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
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

   public SecureCacheImpl(AdvancedCache<K, V> delegate) {
      this(delegate, delegate.getAuthorizationManager(), null);
   }

   private SecureCacheImpl(AdvancedCache<K, V> delegate, AuthorizationManager authzManager, Subject subject) {
      this.authzManager = authzManager;
      this.delegate = delegate;
      this.subject = subject;
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
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.startBatch();
   }

   @Override
   public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter,
                               CacheEventConverter<? super K, ? super V, C> converter) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      delegate.addListener(listener, filter, converter);
   }

   @Override
   public void addListener(Object listener, KeyFilter<? super K> filter) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      delegate.addListener(listener, filter);
   }

   @Override
   public void addListener(Object listener) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      delegate.addListener(listener);
   }

   @Override
   public <C> void addFilteredListener(Object listener,
                                       CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                       Set<Class<? extends Annotation>> filterAnnotations) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      delegate.addFilteredListener(listener, filter, converter, filterAnnotations);
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
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putAsync(key, value);
   }

   @Override
   public void endBatch(boolean successful) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.endBatch(successful);
   }

   @Override
   public void removeListener(Object listener) {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      delegate.removeListener(listener);
   }

   @Override
   public Set<Object> getListeners() {
      authzManager.checkPermission(subject, AuthorizationPermission.LISTEN);
      return delegate.getListeners();
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putAsync(key, value, lifespan, unit);
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      return new SecureCacheImpl(delegate.withFlags(flags), authzManager, subject);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putIfAbsent(key, value);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
                                        TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
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
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.put(key, value);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putAllAsync(data);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.put(key, value, lifespan, unit);
   }

   @Override
   public boolean remove(Object key, Object value) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.remove(key, value);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putAllAsync(data, lifespan, unit);
   }

   @Override
   public void addInterceptor(CommandInterceptor i, int position) {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      delegate.addInterceptor(i, position);
   }

   @Override
   public AsyncInterceptorChain getAsyncInterceptorChain() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getAsyncInterceptorChain();
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putIfAbsent(key, value, lifespan, unit);
   }

   @Override
   public boolean addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.addInterceptorAfter(i, afterInterceptor);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit,
                                              long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.putAll(map, lifespan, unit);
   }

   @Override
   public boolean addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.addInterceptorBefore(i, beforeInterceptor);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replace(key, oldValue, newValue);
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_WRITE);
      return delegate.clearAsync();
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replace(key, value, lifespan, unit);
   }

   @Override
   public void removeInterceptor(int position) {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      delegate.removeInterceptor(position);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putIfAbsentAsync(key, value);
   }

   @Override
   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      delegate.removeInterceptor(interceptorType);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replace(key, oldValue, value, lifespan, unit);
   }

   @Override
   public List<CommandInterceptor> getInterceptorChain() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getInterceptorChain();
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putIfAbsentAsync(key, value, lifespan, unit);
   }

   @Override
   public V replace(K key, V value) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
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
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.putForExternalRead(key, value);
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.putForExternalRead(key, value);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.compute(key, remappingFunction);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.merge(key, value, remappingFunction);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.merge(key, value, remappingFunction, metadata);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.compute(key, remappingFunction, metadata);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.computeIfPresent(key, remappingFunction);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.computeIfPresent(key, remappingFunction, metadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.computeIfAbsent(key, mappingFunction);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.computeIfAbsent(key, mappingFunction, metadata);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.putForExternalRead(key, value);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
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
      };
   }

   @Override
   public AdvancedCache<K, V> lockAs(Object lockOwner) {
      return new SecureCacheImpl<>(delegate.lockAs(lockOwner));
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
                                                TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public boolean isEmpty() {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.isEmpty();
   }

   @Override
   public boolean lock(K... keys) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.lock(keys);
   }

   @Override
   public boolean containsKey(Object key) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.containsKey(key);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.lock(keys);
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.removeAsync(key);
   }

   @Override
   public boolean containsValue(Object value) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.containsValue(value);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
                      TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.removeAsync(key, value);
   }

   @Override
   public void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.applyDelta(deltaAwareValueKey, delta, locksToAcquire);
   }

   @Override
   public void evict(K key) {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      delegate.evict(key);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, value);
   }

   @Override
   public RpcManager getRpcManager() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
      return delegate.getRpcManager();
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V get(Object key) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.get(key);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, value, lifespan, unit);
   }

   @Override
   public BatchContainer getBatchContainer() {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
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
   public InvocationContextContainer getInvocationContextContainer() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
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
   public void removeExpired(K key, V value, Long lifespan) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.removeExpired(key, value, lifespan);
   }

   @Override
   public AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> encoderClass) {
      return new SecureCacheImpl<>(delegate.withEncoding(encoderClass), authzManager, subject);
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapperClass) {
      return new SecureCacheImpl<>(delegate.withWrapping(wrapperClass), authzManager, subject);
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
   public Encoder getKeyEncoder() {
      return delegate.getKeyEncoder();
   }

   @Override
   public Encoder getValueEncoder() {
      return delegate.getValueEncoder();
   }

   @Override
   public Wrapper getKeyWrapper() {
      return delegate.getKeyWrapper();
   }

   @Override
   public Wrapper getValueWrapper() {
      return delegate.getValueWrapper();
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
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
                          TimeUnit maxIdleTimeUnit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
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
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
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
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.remove(key);
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.getAll(keys);
   }

   @Override
   public LockManager getLockManager() {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.getLockManager();
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, oldValue, newValue);
   }

   @Override
   public Stats getStats() {
      authzManager.checkPermission(subject, AuthorizationPermission.ADMIN);
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
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   @Override
   public CacheCollection<V> values() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_READ);
      return delegate.values();
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return new SecureCacheImpl(delegate.with(classLoader));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit,
                                                  long maxIdle, TimeUnit maxIdleUnit) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
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
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.put(key, value, metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.putAll(m, metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      delegate.putAll(m);
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replace(key, value, metadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.replace(key, oldValue, newValue, metadata);
   }

   @Override
   public void clear() {
      authzManager.checkPermission(subject, AuthorizationPermission.BULK_WRITE);
      delegate.clear();
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putIfAbsent(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      authzManager.checkPermission(subject, AuthorizationPermission.WRITE);
      return delegate.putAsync(key, value, metadata);
   }

   @Override
   public CacheEntry getCacheEntry(Object key) {
      authzManager.checkPermission(subject, AuthorizationPermission.READ);
      return delegate.getCacheEntry(key);
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
      return String.format("SecureCache '%s'", delegate.getName());
   }
}
