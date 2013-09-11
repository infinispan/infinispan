package org.infinispan.api;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.util.concurrent.NotifyingFuture;

/**
 *
 * BasicCache.
 *
 * @author Tristan Tarrant
 * @since 5.1
 * @deprecated use {@link org.infinispan.commons.api.BasicCache} instead
 */
@Deprecated
public interface BasicCache<K, V> extends org.infinispan.commons.api.BasicCache<K, V> {

   /**
    * Asynchronous version of {@link #put(Object, Object)}.  This method does not block on remote calls, even if your
    * cache mode is synchronous.  Has no benefit over {@link #put(Object, Object)} if used in LOCAL mode.
    * <p/>
    *
    * @param key   key to use
    * @param value value to store
    * @return a future containing the old value replaced.
    */
   @Override
   NotifyingFuture<V> putAsync(K key, V value);

   /**
    * Asynchronous version of {@link #put(Object, Object, long, TimeUnit)} .  This method does not block on remote
    * calls, even if your cache mode is synchronous.  Has no benefit over {@link #put(Object, Object, long, TimeUnit)}
    * if used in LOCAL mode.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of entry
    * @param unit     time unit for lifespan
    * @return a future containing the old value replaced
    */
   @Override
   NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit);

   /**
    * Asynchronous version of {@link #put(Object, Object, long, TimeUnit, long, TimeUnit)}.  This method does not block
    * on remote calls, even if your cache mode is synchronous.  Has no benefit over {@link #put(Object, Object, long,
    * TimeUnit, long, TimeUnit)} if used in LOCAL mode.
    *
    * @param key          key to use
    * @param value        value to store
    * @param lifespan     lifespan of entry
    * @param lifespanUnit time unit for lifespan
    * @param maxIdle      the maximum amount of time this key is allowed to be idle for before it is considered as
    *                     expired
    * @param maxIdleUnit  time unit for max idle time
    * @return a future containing the old value replaced
    */
   @Override
   NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Asynchronous version of {@link #putAll(Map)}.  This method does not block on remote calls, even if your cache mode
    * is synchronous.  Has no benefit over {@link #putAll(Map)} if used in LOCAL mode.
    *
    * @param data to store
    * @return a future containing a void return type
    */
   @Override
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data);

   /**
    * Asynchronous version of {@link #putAll(Map, long, TimeUnit)}.  This method does not block on remote calls, even if
    * your cache mode is synchronous.  Has no benefit over {@link #putAll(Map, long, TimeUnit)} if used in LOCAL mode.
    *
    * @param data     to store
    * @param lifespan lifespan of entry
    * @param unit     time unit for lifespan
    * @return a future containing a void return type
    */
   @Override
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit);

   /**
    * Asynchronous version of {@link #putAll(Map, long, TimeUnit, long, TimeUnit)}.  This method does not block on
    * remote calls, even if your cache mode is synchronous.  Has no benefit over {@link #putAll(Map, long, TimeUnit,
    * long, TimeUnit)} if used in LOCAL mode.
    *
    * @param data         to store
    * @param lifespan     lifespan of entry
    * @param lifespanUnit time unit for lifespan
    * @param maxIdle      the maximum amount of time this key is allowed to be idle for before it is considered as
    *                     expired
    * @param maxIdleUnit  time unit for max idle time
    * @return a future containing a void return type
    */
   @Override
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Asynchronous version of {@link #clear()}.  This method does not block on remote calls, even if your cache mode is
    * synchronous.  Has no benefit over {@link #clear()} if used in LOCAL mode.
    *
    * @return a future containing a void return type
    */
   @Override
   NotifyingFuture<Void> clearAsync();

   /**
    * Asynchronous version of {@link #putIfAbsent(Object, Object)}.  This method does not block on remote calls, even if
    * your cache mode is synchronous.  Has no benefit over {@link #putIfAbsent(Object, Object)} if used in LOCAL mode.
    * <p/>
    *
    * @param key   key to use
    * @param value value to store
    * @return a future containing the old value replaced.
    */
   @Override
   NotifyingFuture<V> putIfAbsentAsync(K key, V value);

   /**
    * Asynchronous version of {@link #putIfAbsent(Object, Object, long, TimeUnit)} .  This method does not block on
    * remote calls, even if your cache mode is synchronous.  Has no benefit over {@link #putIfAbsent(Object, Object,
    * long, TimeUnit)} if used in LOCAL mode.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of entry
    * @param unit     time unit for lifespan
    * @return a future containing the old value replaced
    */
   @Override
   NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit);

   /**
    * Asynchronous version of {@link #putIfAbsent(Object, Object, long, TimeUnit, long, TimeUnit)}.  This method does
    * not block on remote calls, even if your cache mode is synchronous.  Has no benefit over {@link
    * #putIfAbsent(Object, Object, long, TimeUnit, long, TimeUnit)} if used in LOCAL mode.
    *
    * @param key          key to use
    * @param value        value to store
    * @param lifespan     lifespan of entry
    * @param lifespanUnit time unit for lifespan
    * @param maxIdle      the maximum amount of time this key is allowed to be idle for before it is considered as
    *                     expired
    * @param maxIdleUnit  time unit for max idle time
    * @return a future containing the old value replaced
    */
   @Override
   NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Asynchronous version of {@link #remove(Object)}.  This method does not block on remote calls, even if your cache
    * mode is synchronous.  Has no benefit over {@link #remove(Object)} if used in LOCAL mode.
    *
    * @param key key to remove
    * @return a future containing the value removed
    */
   @Override
   NotifyingFuture<V> removeAsync(Object key);

   /**
    * Asynchronous version of {@link #remove(Object, Object)}.  This method does not block on remote calls, even if your
    * cache mode is synchronous.  Has no benefit over {@link #remove(Object, Object)} if used in LOCAL mode.
    *
    * @param key   key to remove
    * @param value value to match on
    * @return a future containing a boolean, indicating whether the entry was removed or not
    */
   @Override
   NotifyingFuture<Boolean> removeAsync(Object key, Object value);

   /**
    * Asynchronous version of {@link #replace(Object, Object)}.  This method does not block on remote calls, even if
    * your cache mode is synchronous.  Has no benefit over {@link #replace(Object, Object)} if used in LOCAL mode.
    *
    * @param key   key to remove
    * @param value value to store
    * @return a future containing the previous value overwritten
    */
   @Override
   NotifyingFuture<V> replaceAsync(K key, V value);

   /**
    * Asynchronous version of {@link #replace(Object, Object, long, TimeUnit)}.  This method does not block on remote
    * calls, even if your cache mode is synchronous.  Has no benefit over {@link #replace(Object, Object, long,
    * TimeUnit)} if used in LOCAL mode.
    *
    * @param key      key to remove
    * @param value    value to store
    * @param lifespan lifespan of entry
    * @param unit     time unit for lifespan
    * @return a future containing the previous value overwritten
    */
   @Override
   NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit);

   /**
    * Asynchronous version of {@link #replace(Object, Object, long, TimeUnit, long, TimeUnit)}.  This method does not
    * block on remote calls, even if your cache mode is synchronous.  Has no benefit over {@link #replace(Object,
    * Object, long, TimeUnit, long, TimeUnit)} if used in LOCAL mode.
    *
    * @param key          key to remove
    * @param value        value to store
    * @param lifespan     lifespan of entry
    * @param lifespanUnit time unit for lifespan
    * @param maxIdle      the maximum amount of time this key is allowed to be idle for before it is considered as
    *                     expired
    * @param maxIdleUnit  time unit for max idle time
    * @return a future containing the previous value overwritten
    */
   @Override
   NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Asynchronous version of {@link #replace(Object, Object, Object)}.  This method does not block on remote calls,
    * even if your cache mode is synchronous.  Has no benefit over {@link #replace(Object, Object, Object)} if used in
    * LOCAL mode.
    *
    * @param key      key to remove
    * @param oldValue value to overwrite
    * @param newValue value to store
    * @return a future containing a boolean, indicating whether the entry was replaced or not
    */
   @Override
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue);

   /**
    * Asynchronous version of {@link #replace(Object, Object, Object, long, TimeUnit)}.  This method does not block on
    * remote calls, even if your cache mode is synchronous.  Has no benefit over {@link #replace(Object, Object, Object,
    * long, TimeUnit)} if used in LOCAL mode.
    *
    * @param key      key to remove
    * @param oldValue value to overwrite
    * @param newValue value to store
    * @param lifespan lifespan of entry
    * @param unit     time unit for lifespan
    * @return a future containing a boolean, indicating whether the entry was replaced or not
    */
   @Override
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit);

   /**
    * Asynchronous version of {@link #replace(Object, Object, Object, long, TimeUnit, long, TimeUnit)}.  This method
    * does not block on remote calls, even if your cache mode is synchronous.  Has no benefit over {@link
    * #replace(Object, Object, Object, long, TimeUnit, long, TimeUnit)} if used in LOCAL mode.
    *
    * @param key          key to remove
    * @param oldValue     value to overwrite
    * @param newValue     value to store
    * @param lifespan     lifespan of entry
    * @param lifespanUnit time unit for lifespan
    * @param maxIdle      the maximum amount of time this key is allowed to be idle for before it is considered as
    *                     expired
    * @param maxIdleUnit  time unit for max idle time
    * @return a future containing a boolean, indicating whether the entry was replaced or not
    */
   @Override
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Asynchronous version of {@link #get(Object)} that allows user code to
    * retrieve the value associated with a key at a later stage, hence allowing
    * multiple parallel get requests to be sent. Normally, when this method
    * detects that the value is likely to be retrieved from from a remote
    * entity, it will span a different thread in order to allow the
    * asynchronous get call to return immediately. If the call will definitely
    * resolve locally, for example when the cache is configured with LOCAL mode
    * and no stores are configured, the get asynchronous call will act
    * sequentially and will have no different to {@link #get(Object)}.
    *
    * @param key key to retrieve
    * @return a future that can be used to retrieve value associated with the
    * key when this is available. The actual value returned by the future
    * follows the same rules as {@link #get(Object)}
    */
   @Override
   NotifyingFuture<V> getAsync(K key);
}
