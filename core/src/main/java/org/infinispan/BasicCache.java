/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan;

import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * BasicCache provides the common building block for the two different types of caches that Infinispan provides: 
 * embedded and remote. 
 * <p/>
 * For convenience, BasicCache extends {@link ConcurrentMap} and implements all methods accordingly, although methods like
 * {@link ConcurrentMap#keySet()}, {@link ConcurrentMap#values()} and {@link ConcurrentMap#entrySet()} are expensive
 * (prohibitively so when using a distributed cache) and frequent use of these methods is not recommended.
 * <p /> 
 * Other methods such as {@link #size()} provide an approximation-only, and should not be relied on for an accurate picture
 * as to the size of the entire, distributed cache.  Remote nodes are <i>not</i> queried and in-fly transactions are not
 * taken into account, even if {@link #size()} is invoked from within such a transaction.
 * <p/>
 * Also, like many {@link ConcurrentMap} implementations, BasicCache does not support the use of <tt>null</tt> keys or
 * values.
 * <p/>
 * <h3>Unsupported operations</h3>
 * <p>{@link #containsValue(Object)}</p>
 * <h3>Asynchronous operations</h3> BasicCache also supports the use of "async" remote operations.  Note that these methods
 * only really make sense if you are using a clustered cache.  I.e., when used in LOCAL mode, these "async" operations
 * offer no benefit whatsoever.  These methods, such as {@link #putAsync(Object, Object)} offer the best of both worlds
 * between a fully synchronous and a fully asynchronous cache in that a {@link NotifyingFuture} is returned.  The
 * <tt>NotifyingFuture</tt> can then be ignored or thrown away for typical asynchronous behaviour, or queried for
 * synchronous behaviour, which would block until any remote calls complete.  Note that all remote calls are, as far as
 * the transport is concerned, synchronous.  This allows you the guarantees that remote calls succeed, while not
 * blocking your application thread unnecessarily.  For example, usage such as the following could benefit from the
 * async operations:
 * <pre>
 *   NotifyingFuture f1 = cache.putAsync("key1", "value1");
 *   NotifyingFuture f2 = cache.putAsync("key2", "value2");
 *   NotifyingFuture f3 = cache.putAsync("key3", "value3");
 *   f1.get();
 *   f2.get();
 *   f3.get();
 * </pre>
 * The net result is behavior similar to synchronous RPC calls in that at the end, you have guarantees that all calls
 * completed successfully, but you have the added benefit that the three calls could happen in parallel.  This is
 * especially advantageous if the cache uses distribution and the three keys map to different cache instances in the
 * cluster.
 * <p/>
 * Also, the use of async operations when within a transaction return your local value only, as expected.  A
 * NotifyingFuture is still returned though for API consistency. 
 * <p/>
 * Please see the <a href="http://www.jboss.org/infinispan/docs">Infinispan documentation</a> and/or the <a
 * href="http://www.jboss.org/community/wiki/5minutetutorialonInfinispan">5 Minute Usage Tutorial</a> for more details.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @see org.infinispan.manager.CacheContainer
 * @see DefaultCacheManager
 * @see <a href="http://www.jboss.org/infinispan/docs">Infinispan documentation</a>
 * @see <a href="http://www.jboss.org/community/wiki/5minutetutorialonInfinispan">5 Minute Usage Tutorial</a>
 * @since 4.0
 */
public interface BasicCache<K, V> extends ConcurrentMap<K, V>, Lifecycle {
   /**
    * Retrieves the name of the cache
    *
    * @return the name of the cache
    */
   String getName();

   /**
    * Retrieves the version of Infinispan
    *
    * @return a version string
    */
   String getVersion();

   /**
    * An overloaded form of {@link #put(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V put(K key, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #putIfAbsent(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V putIfAbsent(K key, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #putAll(Map)}, which takes in lifespan parameters.  Note that the lifespan is applied
    * to all mappings in the map passed in.
    *
    * @param map      map containing mappings to enter
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    */
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #replace(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V replace(K key, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #replace(Object, Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param oldValue value to replace
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return true if the value was replaced, false otherwise
    */
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #put(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key             key to use
    * @param value           value to store
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * An overloaded form of {@link #putIfAbsent(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key             key to use
    * @param value           value to store
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * An overloaded form of {@link #putAll(Map)}, which takes in lifespan parameters.  Note that the lifespan is applied
    * to all mappings in the map passed in.
    *
    * @param map             map containing mappings to enter
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    */
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * An overloaded form of {@link #replace(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key             key to use
    * @param value           value to store
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * An overloaded form of {@link #replace(Object, Object, Object)}, which takes in lifespan parameters.
    *
    * @param key             key to use
    * @param oldValue        value to replace
    * @param value           value to store
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return true if the value was replaced, false otherwise
    */
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * Asynchronous version of {@link #put(Object, Object)}.  This method does not block on remote calls, even if your
    * cache mode is synchronous.  Has no benefit over {@link #put(Object, Object)} if used in LOCAL mode.
    * <p/>
    *
    * @param key   key to use
    * @param value value to store
    * @return a future containing the old value replaced.
    */
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
   NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Asynchronous version of {@link #putAll(Map)}.  This method does not block on remote calls, even if your cache mode
    * is synchronous.  Has no benefit over {@link #putAll(Map)} if used in LOCAL mode.
    *
    * @param data to store
    * @return a future containing a void return type
    */
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
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Asynchronous version of {@link #clear()}.  This method does not block on remote calls, even if your cache mode is
    * synchronous.  Has no benefit over {@link #clear()} if used in LOCAL mode.
    *
    * @return a future containing a void return type
    */
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
   NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Asynchronous version of {@link #remove(Object)}.  This method does not block on remote calls, even if your cache
    * mode is synchronous.  Has no benefit over {@link #remove(Object)} if used in LOCAL mode.
    *
    * @param key key to remove
    * @return a future containing the value removed
    */
   NotifyingFuture<V> removeAsync(Object key);

   /**
    * Asynchronous version of {@link #remove(Object, Object)}.  This method does not block on remote calls, even if your
    * cache mode is synchronous.  Has no benefit over {@link #remove(Object, Object)} if used in LOCAL mode.
    *
    * @param key   key to remove
    * @param value value to match on
    * @return a future containing a boolean, indicating whether the entry was removed or not
    */
   NotifyingFuture<Boolean> removeAsync(Object key, Object value);

   /**
    * Asynchronous version of {@link #replace(Object, Object)}.  This method does not block on remote calls, even if
    * your cache mode is synchronous.  Has no benefit over {@link #replace(Object, Object)} if used in LOCAL mode.
    *
    * @param key   key to remove
    * @param value value to store
    * @return a future containing the previous value overwritten
    */
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
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Asynchronous version of {@link #get(Object)} that allows user code to
    * retrieve the value associated with a key at a later stage, hence allowing
    * multiple parallel get requests to be sent. Normally, when this method
    * detects that the value is likely to be retrieved from from a remote
    * entity, it will span a different thread in order to allow the
    * asynchronous get call to return immediately. If the call will definitely
    * resolve locally, for example when the cache is configured with LOCAL mode
    * and no cache loaders are configured, the get asynchronous call will act
    * sequentially and will have no different to {@link #get(Object)}.
    *
    * @param key key to retrieve
    * @return a future that can be used to retrieve value associated with the
    * key when this is available. The actual value returned by the future
    * follows the same rules as {@link #get(Object)}
    */
   NotifyingFuture<V> getAsync(K key);
}
