package org.infinispan.cache.impl;

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
import org.infinispan.CacheSet;
import org.infinispan.LockedStream;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.metadata.Metadata;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.stats.Stats;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * Similar to {@link org.infinispan.cache.impl.AbstractDelegatingCache}, but for {@link AdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 * @see org.infinispan.cache.impl.AbstractDelegatingCache
 */
public abstract class AbstractDelegatingAdvancedCache<K, V> extends AbstractDelegatingCache<K, V> implements AdvancedCache<K, V> {

   protected final AdvancedCache<K, V> cache;

   protected AbstractDelegatingAdvancedCache(AdvancedCache<K, V> cache) {
      super(cache);
      this.cache = cache;
   }

   /**
    * @deprecated Since 10.0, will be removed without a replacement
    */
   @Deprecated
   @Override
   public AsyncInterceptorChain getAsyncInterceptorChain() {
      return cache.getAsyncInterceptorChain();
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      //We need to override the super implementation which returns to the decorated cache;
      //otherwise the current operation breaks out of the selected ClassLoader.
      return this;
   }

   @Override
   public EvictionManager getEvictionManager() {
      return cache.getEvictionManager();
   }

   @Override
   public ExpirationManager<K, V> getExpirationManager() {
      return cache.getExpirationManager();
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
   public AuthorizationManager getAuthorizationManager() {
      return cache.getAuthorizationManager();
   }

   @Override
   public AdvancedCache<K, V> lockAs(Object lockOwner) {
      AdvancedCache<K, V> lockCache = cache.lockAs(lockOwner);
      if (lockCache != cache) {
         return rewrap(lockCache);
      } else {
         return this;
      }
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
   public DataContainer<K, V> getDataContainer() {
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
   public AvailabilityMode getAvailability() {
      return cache.getAvailability();
   }

   @Override
   public void setAvailability(AvailabilityMode availabilityMode) {
      cache.setAvailability(availabilityMode);
   }

   @ManagedAttribute(
         description = "Returns the cache availability",
         displayName = "Cache availability",
         dataType = DataType.TRAIT,
         writable = true
   )
   public String getCacheAvailability() {
      return getAvailability().toString();
   }

   public void setCacheAvailability(String availabilityString) {
      setAvailability(AvailabilityMode.valueOf(availabilityString));
   }

   @ManagedAttribute(
         description = "Returns whether cache rebalancing is enabled",
         displayName = "Cache rebalacing",
         dataType = DataType.TRAIT,
         writable = true
   )
   public boolean isRebalancingEnabled() {
      LocalTopologyManager localTopologyManager = getComponentRegistry().getComponent(LocalTopologyManager.class);
      if (localTopologyManager != null) {
         try {
            return localTopologyManager.isCacheRebalancingEnabled(getName());
         } catch (Exception e) {
            throw new CacheException(e);
         }
      } else {
         return false;
      }
   }

   public void setRebalancingEnabled(boolean enabled) {
      LocalTopologyManager localTopologyManager = getComponentRegistry().getComponent(LocalTopologyManager.class);
      if (localTopologyManager != null) {
         try {
            localTopologyManager.setCacheRebalancingEnabled(getName(), enabled);
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, boolean touchEvenIfExpired) {
      return cache.touch(key, touchEvenIfExpired);
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, int segment, boolean touchEvenIfExpired) {
      return cache.touch(key, segment, touchEvenIfExpired);
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag flag) {
      AdvancedCache<K, V> flagCache = cache.withFlags(flag);
      if (flagCache != cache) {
         return rewrap(flagCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      AdvancedCache<K, V> flagCache = cache.withFlags(flags);
      if (flagCache != cache) {
         return rewrap(flagCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<K, V> withFlags(Collection<Flag> flags) {
      AdvancedCache<K, V> flagCache = cache.withFlags(flags);
      if (flagCache != cache) {
         return rewrap(flagCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<K, V> noFlags() {
      AdvancedCache<K, V> flagCache = cache.noFlags();
      if (flagCache != cache) {
         return rewrap(flagCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<K, V> transform(Function<AdvancedCache<K, V>, ? extends AdvancedCache<K, V>> transformation) {
      AdvancedCache<K, V> newDelegate = cache.transform(transformation);
      AdvancedCache<K, V> newInstance = newDelegate != cache ? rewrap(newDelegate) : this;
      return transformation.apply(newInstance);
   }

   @Override
   public AdvancedCache<K, V> withSubject(Subject subject) {
      AdvancedCache<K, V> newDelegate = cache.withSubject(subject);
      if (newDelegate != cache) {
         return rewrap(newDelegate);
      } else {
         return this;
      }
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
   public Stats getStats() {
      return cache.getStats();
   }

   @Override
   public ClassLoader getClassLoader() {
      return cache.getClassLoader();
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return this;
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      return cache.getAll(keys);
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(Object key) {
      return cache.getCacheEntry(key);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> getCacheEntryAsync(Object key) {
      return cache.getCacheEntryAsync(key);
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      return cache.getAllCacheEntries(keys);
   }

   @Override
   public Map<K, V> getAndPutAll(Map<? extends K, ? extends V> map) {
      return cache.getAndPutAll(map);
   }

   @Override
   public java.util.Map<K, V> getGroup(String groupName) {
      return cache.getGroup(groupName);
   }

   @Override
   public void removeGroup(String groupName) {
      cache.removeGroup(groupName);
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      return cache.put(key, value, metadata);
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      return cache.replace(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, Metadata metadata) {
      return cache.replaceAsync(key, value, metadata);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> replaceAsyncEntry(K key, V value, Metadata metadata) {
      return cache.replaceAsyncEntry(key, value, metadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return cache.replace(key, oldValue, value, metadata);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, Metadata metadata) {
      return cache.replaceAsync(key, oldValue, newValue, metadata);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      return cache.putIfAbsent(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, Metadata metadata) {
      return cache.putIfAbsentAsync(key, value, metadata);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putIfAbsentAsyncEntry(K key, V value, Metadata metadata) {
      return cache.putIfAbsentAsyncEntry(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return cache.putAsync(key, value, metadata);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putAsyncEntry(K key, V value, Metadata metadata) {
      return cache.putAsyncEntry(key, value, metadata);
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      cache.putForExternalRead(key, value, metadata);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.compute(key, remappingFunction, metadata);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.computeIfPresent(key, remappingFunction, metadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return cache.computeIfAbsent(key, mappingFunction, metadata);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.merge(key, value, remappingFunction, metadata);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.computeAsync(key, remappingFunction, metadata);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.computeIfPresentAsync(key, remappingFunction, metadata);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return cache.computeIfAbsentAsync(key, mappingFunction, metadata);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.mergeAsync(key, value, remappingFunction, metadata);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return cache.computeAsync(key, remappingFunction);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeAsync(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.computeAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction) {
      return cache.computeIfAbsentAsync(key, mappingFunction);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return cache.computeIfPresentAsync(key, remappingFunction);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return cache.mergeAsync(key, value, remappingFunction);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      cache.putAll(map, metadata);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, Metadata metadata) {
      return cache.putAllAsync(map, metadata);
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      return cache.cacheEntrySet();
   }

   @Override
   public LockedStream<K, V> lockedStream() {
      return cache.lockedStream();
   }

   @Override
   public CompletableFuture<Boolean> removeLifespanExpired(K key, V value, Long lifespan) {
      return cache.removeLifespanExpired(key, value, lifespan);
   }

   @Override
   public CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value) {
      return cache.removeMaxIdleExpired(key, value);
   }

   @Override
   public AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> encoder) {
      AdvancedCache encoderCache = cache.withEncoding(encoder);
      if (encoderCache != cache) {
         return rewrap(encoderCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache withEncoding(Class<? extends Encoder> keyEncoder, Class<? extends Encoder> valueEncoder) {
      AdvancedCache encoderCache = cache.withEncoding(keyEncoder, valueEncoder);
      if (encoderCache != cache) {
         return rewrap(encoderCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<?, ?> withKeyEncoding(Class<? extends Encoder> encoder) {
      AdvancedCache encoderCache = cache.withKeyEncoding(encoder);
      if (encoderCache != cache) {
         return rewrap(encoderCache);
      } else {
         return this;
      }
   }

   @Deprecated
   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapper) {
      AdvancedCache<K, V> encoderCache = cache.withWrapping(wrapper);
      if (encoderCache != cache) {
         return this.rewrap(encoderCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<?, ?> withMediaType(String keyMediaType, String valueMediaType) {
      AdvancedCache encoderCache = this.cache.withMediaType(keyMediaType, valueMediaType);
      if (encoderCache != cache) {
         return rewrap(encoderCache);
      } else {
         return this;
      }
   }

   @Override
   public <K1, V1> AdvancedCache<K1, V1> withMediaType(MediaType keyMediaType, MediaType valueMediaType) {
      AdvancedCache encoderCache = this.cache.withMediaType(keyMediaType, valueMediaType);
      if (encoderCache != cache) {
         return rewrap(encoderCache);
      } else {
         return (AdvancedCache<K1, V1>) this;
      }
   }

   @Override
   public AdvancedCache<K, V> withStorageMediaType() {
      AdvancedCache<K, V> encoderCache = this.cache.withStorageMediaType();
      if (encoderCache != cache) {
         return rewrap(encoderCache);
      } else {
         return this;
      }
   }

   @Deprecated
   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapper, Class<? extends Wrapper> valueWrapper) {
      AdvancedCache<K, V> encoderCache = cache.withWrapping(keyWrapper, valueWrapper);
      if (encoderCache != cache) {
         return rewrap(encoderCache);
      } else {
         return this;
      }
   }

   /**
    * No generics because some methods return {@code AdvancedCache<?, ?>},
    * and returning the proper type would require erasure anyway.
    */
   public abstract AdvancedCache rewrap(AdvancedCache newDelegate);

   @Override
   public DataConversion getKeyDataConversion() {
      return cache.getKeyDataConversion();
   }

   @Override
   public DataConversion getValueDataConversion() {
      return cache.getValueDataConversion();
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      return cache.getAllAsync(keys);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> removeAsyncEntry(Object key) {
      return cache.removeAsyncEntry(key);
   }
}
