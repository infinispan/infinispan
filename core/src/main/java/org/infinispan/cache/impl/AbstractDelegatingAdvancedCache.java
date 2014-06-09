package org.infinispan.cache.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.iteration.EntryIterable;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.stats.Stats;
import org.infinispan.util.concurrent.locks.LockManager;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

/**
 * Similar to {@link org.infinispan.cache.impl.AbstractDelegatingCache}, but for {@link AdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 * @see org.infinispan.cache.impl.AbstractDelegatingCache
 */
public class AbstractDelegatingAdvancedCache<K, V> extends AbstractDelegatingCache<K, V> implements AdvancedCache<K, V> {

   protected final AdvancedCache<K, V> cache;
   private final AdvancedCacheWrapper<K, V> wrapper;

   public AbstractDelegatingAdvancedCache(final AdvancedCache<K, V> cache) {
      this(cache, new AdvancedCacheWrapper<K, V>() {
         @Override
         public AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache) {
            return new AbstractDelegatingAdvancedCache<K, V>(cache);
         }
      });
   }

   public AbstractDelegatingAdvancedCache(
         AdvancedCache<K, V> cache, AdvancedCacheWrapper<K, V> wrapper) {
      super(cache);
      this.cache = cache;
      this.wrapper = wrapper;
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
   public AuthorizationManager getAuthorizationManager() {
      return cache.getAuthorizationManager();
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
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      return this.wrapper.wrap(this.cache.withFlags(flags));
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
      return this.wrapper.wrap(this.cache.with(classLoader));
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(K key) {
      return cache.getCacheEntry(key);
   }

   @Override
   public EntryIterable<K, V> filterEntries(KeyValueFilter<? super K, ? super V> filter) {
      return cache.filterEntries(filter);
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
   public NotifyingFuture<V> putAsync(K key, V value, Metadata metadata) {
      return cache.putAsync(key, value, metadata);
   }

   protected final void putForExternalRead(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      ((CacheImpl<K, V>) cache).putForExternalRead(key, value, flags, classLoader);
   }

   public interface AdvancedCacheWrapper<K, V> {
      AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache);
   }

}
