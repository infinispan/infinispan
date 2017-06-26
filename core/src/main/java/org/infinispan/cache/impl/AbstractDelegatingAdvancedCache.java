package org.infinispan.cache.impl;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.security.auth.Subject;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheSet;
import org.infinispan.LockedStream;
import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
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
@MBean(objectName = CacheImpl.OBJECT_NAME, description = "Component that represents an individual cache instance.")
public class AbstractDelegatingAdvancedCache<K, V> extends AbstractDelegatingCache<K, V> implements AdvancedCache<K, V> {

   protected final AdvancedCache<K, V> cache;
   private final AdvancedCacheWrapper<K, V> wrapper;

   public AbstractDelegatingAdvancedCache(final AdvancedCache<K, V> cache) {
      this(cache, AbstractDelegatingAdvancedCache::new);
   }

   protected AbstractDelegatingAdvancedCache(AdvancedCache<K, V> cache, AdvancedCacheWrapper<K, V> wrapper) {
      super(cache);
      this.cache = cache;
      this.wrapper = wrapper;
   }

   @Override
   public void addInterceptor(CommandInterceptor i, int position) {
      cache.getAsyncInterceptorChain().addInterceptor(i, position);
   }

   @Override
   public AsyncInterceptorChain getAsyncInterceptorChain() {
      return cache.getAsyncInterceptorChain();
   }

   @Override
   public boolean addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      return cache.getAsyncInterceptorChain().addInterceptorAfter(i, afterInterceptor);
   }

   @Override
   public boolean addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      return cache.getAsyncInterceptorChain().addInterceptorBefore(i, beforeInterceptor);
   }

   @Override
   public void removeInterceptor(int position) {
      cache.getAsyncInterceptorChain().removeInterceptor(position);
   }

   @Override
   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      cache.getAsyncInterceptorChain().removeInterceptor(interceptorType);
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
      AdvancedCache<K, V> lockCache = this.cache.lockAs(lockOwner);
      if (lockCache != cache) {
         return this.wrapper.wrap(lockCache);
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
   public InvocationContextContainer getInvocationContextContainer() {
      return cache.getInvocationContextContainer();
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

   public void setCacheAvailability(String availabilityString) throws Exception {
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
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      AdvancedCache<K, V> flagCache = this.cache.withFlags(flags);
      if (flagCache != cache) {
         return this.wrapper.wrap(flagCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<K, V> withSubject(Subject subject) {
      return this.wrapper.wrap(cache.withSubject(subject));
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
   public void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire) {
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
      AdvancedCache<K, V> loaderCache = this.cache.with(classLoader);
      if (loaderCache != cache) {
         return this.wrapper.wrap(loaderCache);
      } else {
         return this;
      }
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
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return cache.replace(key, oldValue, value, metadata);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      return cache.putIfAbsent(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return cache.putAsync(key, value, metadata);
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
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      cache.putAll(map, metadata);
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
   public void removeExpired(K key, V value, Long lifespan) {
      cache.removeExpired(key, value, lifespan);
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> encoder) {
      AdvancedCache encoderCache = this.cache.withEncoding(encoder);
      if (encoderCache != cache) {
         return this.wrapper.wrap(encoderCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> keyEncoder, Class<? extends Encoder> valueEncoder) {
      AdvancedCache encoderCache = this.cache.withEncoding(keyEncoder, valueEncoder);
      if (encoderCache != cache) {
         return this.wrapper.wrap(encoderCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapper) {
      AdvancedCache encoderCache = this.cache.withWrapping(wrapper);
      if (encoderCache != cache) {
         return this.wrapper.wrap(encoderCache);
      } else {
         return this;
      }
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapper, Class<? extends Wrapper> valueWrapper) {
      AdvancedCache encoderCache = this.cache.withWrapping(keyWrapper, valueWrapper);
      if (encoderCache != cache) {
         return this.wrapper.wrap(encoderCache);
      } else {
         return this;
      }
   }

   @Override
   public Encoder getKeyEncoder() {
      return cache.getKeyEncoder();
   }

   @Override
   public Encoder getValueEncoder() {
      return cache.getValueEncoder();
   }

   @Override
   public Wrapper getKeyWrapper() {
      return cache.getKeyWrapper();
   }

   @Override
   public Wrapper getValueWrapper() {
      return cache.getValueWrapper();
   }

   protected final void putForExternalRead(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      ((CacheImpl<K, V>) cache).putForExternalRead(key, value, EnumUtil.bitSetOf(flags));
   }

   protected final void putForExternalRead(K key, V value, Metadata metadata, EnumSet<Flag> flags, ClassLoader classLoader) {
      ((CacheImpl<K, V>) cache).putForExternalRead(key, value, metadata, EnumUtil.bitSetOf(flags));
   }

   public interface AdvancedCacheWrapper<K, V> {
      AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache);
   }
}
