package org.horizon;

import org.horizon.batch.BatchContainer;
import org.horizon.container.DataContainer;
import org.horizon.eviction.EvictionManager;
import org.horizon.factories.ComponentRegistry;
import org.horizon.interceptors.base.CommandInterceptor;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.invocation.Options;

import java.util.List;
import java.util.Map;
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
    * @return an immutable {@link java.util.List} of {@link org.horizon.interceptors.base.CommandInterceptor}s
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

   BatchContainer getBatchContainer();

   InvocationContextContainer getInvocationContextContainer();

   DataContainer getDataContainer();

   void putForExternalRead(K key, V value, Options... options);

   V put(K key, V value, Options... options);

   V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Options... options);

   V putIfAbsent(K key, V value, Options... options);

   V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Options... options);

   void putAll(Map<? extends K, ? extends V> map, Options... options);

   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Options... options);

   V remove(Object key, Options... options);

   boolean remove(Object key, Object oldValue, Options... options);

   void clear(Options... options);

   boolean containsKey(Object key, Options... options);

   V get(Object key, Options... options);
}
