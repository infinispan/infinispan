/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

import org.infinispan.config.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.manager.CacheManager;
import org.infinispan.notifications.Listenable;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * The central interface of Infinispan.  A Cache provides a highly concurrent, optionally distributed data structure
 * with additional features such as: <ul> <li>JTA transaction compatibility</li> <li>Eviction support to prevent
 * evicting entries from memory to prevent {@link OutOfMemoryError}s</li> <li>Persisting entries to a {@link
 * org.infinispan.loader.CacheStore}, either when they are evicted as an overflow, or all the time, to maintain
 * persistent copies that would withstand server failure or restarts.</li> </ul>
 * <p/>
 * For convenience, Cache extends {@link java.util.concurrent.ConcurrentMap} and implements all methods accordingly,
 * although methods like {@link java.util.concurrent.ConcurrentMap#keySet()}, {@link
 * java.util.concurrent.ConcurrentMap#values()} and {@link java.util.concurrent.ConcurrentMap#entrySet()} are expensive
 * (prohibitively so when using a distributed cache) and frequent use of these methods is not recommended.
 * <p/>
 * Also, like most {@link java.util.concurrent.ConcurrentMap} implementations, Cache does not support the use of
 * <tt>null</tt> keys (although <tt>null</tt> values are allowed).
 * <p/>
 * Please see the <a href="http://www.jboss.org/infinispan/docs">Infinispan documentation</a> for more details.
 * <p/>
 * A note about return values.  Certain methods on {@link Map} could have indeterminate return values, <i>if</i> the
 * DISTRIBITION cache mode is used in an asynchronous manner <i>and</i> the invocation takes place on a cache instance
 * where the entry involved is not local.  These methods are: <ul> <li>{@link #put(Object, Object)} and its variations,
 * including {@link #putIfAbsent(Object, Object)}</li> <li>{@link #remove(Object)} and its variations</li> <li>{@link
 * #replace(Object, Object)} and its variations</li> </ul> The methods still <i>behave</i> as expected, i.e., {@link
 * #putIfAbsent(Object, Object)} will be a no-op if there is a value present, just that since remote calls are
 * asynchronous and the operation needs to be executed on a remote cache, the calling cache will not wait for a return
 * value.
 * <p/>
 * As such, as a general rule of thumb, return values to these methods should <i>not</i> be used if using DISTRIBUTION
 * in asynchronous mode.
 * <p/>
 * For all other cache modes (including <i>synchronous</i> DISTRIBUTION) these return values are reliable and can be
 * used.
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @since 4.0
 */
public interface Cache<K, V> extends ConcurrentMap<K, V>, Lifecycle, Listenable {
   /**
    * Under special operating behavior, associates the value with the specified key. <ul> <li> Only goes through if the
    * key specified does not exist; no-op otherwise (similar to {@link java.util.concurrent.ConcurrentMap#putIfAbsent(Object,
    * Object)})</i> <li> Force asynchronous mode for replication to prevent any blocking.</li> <li> invalidation does
    * not take place. </li> <li> 0ms lock timeout to prevent any blocking here either. If the lock is not acquired, this
    * method is a no-op, and swallows the timeout exception.</li> <li> Ongoing transactions are suspended before this
    * call, so failures here will not affect any ongoing transactions.</li> <li> Errors and exceptions are 'silent' -
    * logged at a much lower level than normal, and this method does not throw exceptions</li> </ul> This method is for
    * caching data that has an external representation in storage, where, concurrent modification and transactions are
    * not a consideration, and failure to put the data in the cache should be treated as a 'suboptimal outcome' rather
    * than a 'failing outcome'.
    * <p/>
    * An example of when this method is useful is when data is read from, for example, a legacy datastore, and is cached
    * before returning the data to the caller.  Subsequent calls would prefer to get the data from the cache and if the
    * data doesn't exist in the cache, fetch again from the legacy datastore.
    * <p/>
    * See <a href="http://jira.jboss.com/jira/browse/JBCACHE-848">JBCACHE-848</a> for details around this feature.
    * <p/>
    *
    * @param key   key with which the specified value is to be associated.
    * @param value value to be associated with the specified key.
    * @throws IllegalStateException if {@link #getStatus()} would not return {@link org.infinispan.lifecycle.ComponentStatus#RUNNING}.
    */
   void putForExternalRead(K key, V value);

   void evict(K key);

   Configuration getConfiguration();

   /**
    * @return true if a batch was successfully started; false if one was available and already running.
    */
   public boolean startBatch();

   public void endBatch(boolean successful);

   String getName();

   String getVersion();

   /**
    * Retrieves the cache manager responsible for creating this cache instance.
    *
    * @return a cache manager
    */
   CacheManager getCacheManager();

   /**
    * An overloaded form of {@link #put(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V put(K key, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #putIfAbsent(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V putIfAbsent(K key, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #putAll(java.util.Map)}, which takes in lifespan parameters.  Note that the lifespan
    * is applied to all mappings in the map passed in.
    *
    * @param map      map containing mappings to enter
    * @param lifespan lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    */
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #replace(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
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
    * @param lifespan lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return true if the value was replaced, false otherwise
    */
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #put(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key             key to use
    * @param value           value to store
    * @param lifespan        lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
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
    * @param lifespan        lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * An overloaded form of {@link #putAll(java.util.Map)}, which takes in lifespan parameters.  Note that the lifespan
    * is applied to all mappings in the map passed in.
    *
    * @param map             map containing mappings to enter
    * @param lifespan        lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
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
    * @param lifespan        lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
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
    * @param lifespan        lifespan of the entry.  Negative values are intepreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return true if the value was replaced, false otherwise
    */
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);


   AdvancedCache<K, V> getAdvancedCache();

   /**
    * Method that releases object references of cached objects held in the cache by serializing them to byte buffers.
    * Cached objects are lazily deserialized when accessed again, based on the calling thread's context class loader.
    * <p/>
    * This can be expensive, based on the effort required to serialize cached objects.
    * <p/>
    */
   void compact();

   ComponentStatus getStatus();
}
