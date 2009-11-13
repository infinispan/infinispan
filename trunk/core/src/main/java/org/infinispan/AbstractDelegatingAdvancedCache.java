package org.infinispan;

import org.infinispan.batch.BatchContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;

import java.util.Collection;
import java.util.List;

/**
 * Similar to {@link org.infinispan.AbstractDelegatingCache}, but for {@link AdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.AbstractDelegatingCache
 */
public abstract class AbstractDelegatingAdvancedCache<K, V> extends AbstractDelegatingCache<K, V> implements AdvancedCache<K, V> {

   private AdvancedCache<K, V> cache;

   public AbstractDelegatingAdvancedCache(AdvancedCache<K, V> cache) {
      super(cache);
      this.cache = cache;
   }

   public void addInterceptor(CommandInterceptor i, int position) {
      cache.addInterceptor(i, position);
   }

   public void addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      cache.addInterceptorAfter(i, afterInterceptor);
   }

   public void addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      cache.addInterceptorBefore(i, beforeInterceptor);
   }

   public void removeInterceptor(int position) {
      cache.removeInterceptor(position);
   }

   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      cache.removeInterceptor(interceptorType);
   }

   public List<CommandInterceptor> getInterceptorChain() {
      return cache.getInterceptorChain();
   }

   public EvictionManager getEvictionManager() {
      return cache.getEvictionManager();
   }

   public ComponentRegistry getComponentRegistry() {
      return cache.getComponentRegistry();
   }

   public RpcManager getRpcManager() {
      return cache.getRpcManager();
   }

   public BatchContainer getBatchContainer() {
      return cache.getBatchContainer();
   }

   public InvocationContextContainer getInvocationContextContainer() {
      return cache.getInvocationContextContainer();
   }

   public DataContainer getDataContainer() {
      return cache.getDataContainer();
   }

   public AdvancedCache<K, V> withFlags(Flag... flags) {
      cache.withFlags(flags);
      return this;
   }

   public void lock(K key) {
      cache.lock(key);
   }

   public void lock(Collection<? extends K> keys) {
      cache.lock(keys);
   }
}
