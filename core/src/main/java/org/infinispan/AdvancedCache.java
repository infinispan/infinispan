package org.infinispan;

import org.infinispan.batch.BatchContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.RpcManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An advanced interface that exposes additional methods not available on {@link Cache}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface AdvancedCache<K, V> extends Cache<K, V> {
   /**
    * Adds a custom interceptor to the interceptor chain, at specified position, where the first interceptor in the
    * chain is at position 0 and the last one at NUM_INTERCEPTORS - 1.
    *
    * @param i        the interceptor to add
    * @param position the position to add the interceptor
    */
   void addInterceptor(CommandInterceptor i, int position);

   /**
    * Adds a custom interceptor to the interceptor chain, after an instance of the specified interceptor type. Throws a
    * cache exception if it cannot find an interceptor of the specified type.
    *
    * @param i                interceptor to add
    * @param afterInterceptor interceptor type after which to place custom interceptor
    */
   void addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor);

   /**
    * Adds a custom interceptor to the interceptor chain, before an instance of the specified interceptor type. Throws a
    * cache exception if it cannot find an interceptor of the specified type.
    *
    * @param i                 interceptor to add
    * @param beforeInterceptor interceptor type before which to place custom interceptor
    */
   void addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor);

   /**
    * Removes the interceptor at a specified position, where the first interceptor in the chain is at position 0 and the
    * last one at getInterceptorChain().size() - 1.
    *
    * @param position the position at which to remove an interceptor
    */
   void removeInterceptor(int position);

   /**
    * Removes the interceptor of specified type.
    *
    * @param interceptorType type of interceptor to remove
    */
   void removeInterceptor(Class<? extends CommandInterceptor> interceptorType);

   /**
    * Retrieves the current Interceptor chain.
    *
    * @return an immutable {@link java.util.List} of {@link org.infinispan.interceptors.base.CommandInterceptor}s
    *         configured for this cache
    */
   List<CommandInterceptor> getInterceptorChain();

   /**
    * @return the eviction manager - if one is configured - for this cache instance
    */
   EvictionManager getEvictionManager();

   /**
    * @return the component registry for this cache instance
    */
   ComponentRegistry getComponentRegistry();

   RpcManager getRpcManager();

   BatchContainer getBatchContainer();

   InvocationContextContainer getInvocationContextContainer();

   DataContainer getDataContainer();

   void putForExternalRead(K key, V value, Flag... flags);

   V put(K key, V value, Flag... flags);

   V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags);

   V putIfAbsent(K key, V value, Flag... flags);

   V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags);

   void putAll(Map<? extends K, ? extends V> map, Flag... flags);

   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags);

   V remove(Object key, Flag... flags);

   void clear(Flag... flags);

   V replace(K k, V v, Flag... flags);

   V replace(K k, V oV, V nV, Flag... flags);

   boolean replace(K k, V v, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags);

   boolean replace(K k, V oV, V nV, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags);


   // -- async methods --
   Future<V> putAsync(K key, V value, Flag... flags);

   Future<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags);

   Future<V> putIfAbsentAsync(K key, V value, Flag... flags);

   Future<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags);

   Future<Void> putAllAsync(Map<? extends K, ? extends V> map, Flag... flags);

   Future<Void> putAllAsync(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags);

   Future<V> removeAsync(Object key, Flag... flags);

   Future<Void> clearAsync(Flag... flags);

   Future<V> replaceAsync(K k, V v, Flag... flags);

   Future<V> replaceAsync(K k, V oV, V nV, Flag... flags);

   Future<Boolean> replaceAsync(K k, V v, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags);

   Future<Boolean> replaceAsync(K k, V oV, V nV, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit, Flag... flags);

   boolean containsKey(Object key, Flag... flags);

   V get(Object key, Flag... flags);
}
