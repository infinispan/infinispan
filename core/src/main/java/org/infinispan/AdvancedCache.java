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

import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.stats.Stats;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.locks.LockManager;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

/**
 * An advanced interface that exposes additional methods not available on {@link Cache}.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface AdvancedCache<K, V> extends Cache<K, V> {

   /**
    * A method that adds flags to any API call.  For example, consider the following code snippet:
    * <pre>
    *   cache.withFlags(Flag.FORCE_WRITE_LOCK).get(key);
    * </pre>
    * will invoke a cache.get() with a write lock forced.
    * <p />
    * <b>Note</b> that for the flag to take effect, the cache operation <b>must</b> be invoked on the instance returned by
    * this method.
    * <p />
    * As an alternative to setting this on every
    * invocation, users could also consider using the {@link DecoratedCache} wrapper, as this allows for more readable
    * code.  E.g.:
    * <pre>
    *    Cache forceWriteLockCache = new DecoratedCache(cache, Flag.FORCE_WRITE_LOCK);
    *    forceWriteLockCache.get(key1);
    *    forceWriteLockCache.get(key2);
    *    forceWriteLockCache.get(key3);
    * </pre>
    *
    * @param flags a set of flags to apply.  See the {@link Flag} documentation.
    * @return an {@link AdvancedCache} instance on which a real operation is to be invoked, if the flags are
    * to be applied.
    */
   AdvancedCache<K, V> withFlags(Flag... flags);

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
    * @return true if successful, false otherwise.
    */
   boolean addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor);

   /**
    * Adds a custom interceptor to the interceptor chain, before an instance of the specified interceptor type. Throws a
    * cache exception if it cannot find an interceptor of the specified type.
    *
    * @param i                 interceptor to add
    * @param beforeInterceptor interceptor type before which to place custom interceptor
    * @return true if successful, false otherwise.
    */
   boolean addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor);

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

   /**
    * Retrieves a reference to the {@link org.infinispan.distribution.DistributionManager} if the cache is configured
    * to use Distribution.  Otherwise, returns a null.
    * @return a DistributionManager, or null.
    */
   DistributionManager getDistributionManager();

   /**
    * Locks a given key or keys eagerly across cache nodes in a cluster.
    * <p>
    * Keys can be locked eagerly in the context of a transaction only.
    *
    * @param keys the keys to lock
    * @return true if the lock acquisition attempt was successful for <i>all</i> keys;
    * false will only be returned if the lock acquisition timed out and the
    * operation has been called with {@link Flag#FAIL_SILENTLY}.
    * @throws org.infinispan.util.concurrent.TimeoutException if the lock
    * cannot be acquired within the configured lock acquisition time.
    */
   boolean lock(K... keys);

   /**
    * Locks collections of keys eagerly across cache nodes in a cluster.
    * <p>
    * Collections of keys can be locked eagerly in the context of a transaction only.
    *
    * @param keys collection of keys to lock
    * @return true if the lock acquisition attempt was successful for <i>all</i> keys;
    * false will only be returned if the lock acquisition timed out and the
    * operation has been called with {@link Flag#FAIL_SILENTLY}.
    * @throws org.infinispan.util.concurrent.TimeoutException if the lock
    * cannot be acquired within the configured lock acquisition time.
    */
   boolean lock(Collection<? extends K> keys);


   /**
    * Applies the given Delta to the DeltaAware object stored under deltaAwareValueKey if and only if all
    * locksToAcquire locks are successfully obtained
    *
    *
    * @param deltaAwareValueKey the key for DeltaAware object
    * @param delta the delta to be applied to DeltaAware object
    * @param locksToAcquire keys to be locked in DeltaAware scope
    */
   void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire);

   /**
    * Returns the component in charge of communication with other caches in
    * the cluster.  If the cache's {@link org.infinispan.config.Configuration.CacheMode}
    * is {@link org.infinispan.config.Configuration.CacheMode#LOCAL}, this
    * method will return null.
    *
    * @return the RPC manager component associated with this cache instance or null
    */
   RpcManager getRpcManager();

   /**
    * Returns the component in charge of batching cache operations.
    *
    * @return the batching component associated with this cache instance
    */
   BatchContainer getBatchContainer();

   /**
    * Returns the component in charge of managing the interactions between the
    * cache operations and the context information associated with them.
    *
    * @return the invocation context container component
    */
   InvocationContextContainer getInvocationContextContainer();

   /**
    * Returns the container where data is stored in the cache. Users should
    * interact with this component with care because direct calls on it bypass
    * the internal interceptors and other infrastructure in place to guarantee
    * the consistency of data.
    *
    * @return the data container associated with this cache instance
    */
   DataContainer getDataContainer();

   /**
    * Returns the transaction manager configured for this cache. If no
    * transaction manager was configured, this method returns null.
    *
    * @return the transaction manager associated with this cache instance or null
    */
   TransactionManager getTransactionManager();

   /**
    * Returns the component that deals with all aspects of acquiring and
    * releasing locks for cache entries.
    *
    * @return retrieves the lock manager associated with this cache instance
    */
   LockManager getLockManager();

   /**
    * Returns a {@link Stats} object that allows several statistics associated
    * with this cache at runtime.
    *
    * @return this cache's {@link Stats} object
    */
   Stats getStats();

   /**
    * Returns the {@link XAResource} associated with this cache which can be
    * used to do transactional recovery.
    *
    * @return an instance of {@link XAResource}
    */
   XAResource getXAResource();

   /**
    * Returns the cache loader associated associated with this cache.  As an alternative to setting this on every
    * invocation, users could also consider using the {@link DecoratedCache} wrapper.
    *
    * @return this cache's cache loader
    */
   ClassLoader getClassLoader();

   /**
    * Using this operation, users can call any {@link AdvancedCache} operation
    * with a given {@link ClassLoader}. This means that any {@link ClassLoader} happening
    * as a result of the cache operation will be done using the {@link ClassLoader}
    * given. For example:
    * <p />
    * When users store POJO instances in caches configured with {@link org.infinispan.config.Configuration#storeAsBinary},
    * these instances are transformed into byte arrays. When these entries are
    * read from the cache, a lazy unmarshalling process happens where these byte
    * arrays are transformed back into POJO instances. Using {@link AdvancedCache#with(ClassLoader)}
    * when reading that enables users to provide the class loader that should
    * be used when trying to locate the classes that are constructed as a result
    * of the unmarshalling process.
    * <pre>
    *    cache.with(classLoader).get(key);
    * </pre>
    * <b>Note</b> that for the flag to take effect, the cache operation <b>must</b> be invoked on the instance
    * returned by this method.
    * <p />
    * As an alternative to setting this on every
    * invocation, users could also consider using the {@link DecoratedCache} wrapper, as this allows for more readable
    * code.  E.g.:
    * <pre>
    *    Cache classLoaderSpecificCache = new DecoratedCache(cache, classLoader);
    *    classLoaderSpecificCache.get(key1);
    *    classLoaderSpecificCache.get(key2);
    *    classLoaderSpecificCache.get(key3);
    * </pre>
    *
    * @return an {@link AdvancedCache} instance upon which operations can be called
    * with a particular {@link ClassLoader}.
    */
   AdvancedCache<K, V> with(ClassLoader classLoader);

   /**
    * @deprecated Use {@link #getCacheEntry(Object)}
    */
   @Deprecated
   CacheEntry getCacheEntry(Object key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader);

   /**
    * An overloaded form of {@link #put(K, V)}, which takes in an instance of
    * {@link Metadata} which can be used to provide metadata information for
    * the entry being stored, such as lifespan, version of value...etc.
    *
    * @param key key to use
    * @param value value to store
    * @param metadata information to store alongside the value
    * @return the previous value associated with <tt>key</tt>, or
    *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
    *
    * @since 5.3
    */
   V put(K key, V value, Metadata metadata);

   /**
    * An overloaded form of {@link #replace(K, V)}, which takes in an
    * instance of {@link Metadata} which can be used to provide metadata
    * information for the entry being stored, such as lifespan, version
    * of value...etc. The {@link Metadata} is only stored if the call is
    * successful.
    *
    * @param key key with which the specified value is associated
    * @param value value to be associated with the specified key
    * @param metadata information to store alongside the new value
    * @return the previous value associated with the specified key, or
    *         <tt>null</tt> if there was no mapping for the key.
    *
    * @since 5.3
    */
   V replace(K key, V value, Metadata metadata);

   /**
    * An overloaded form of {@link #replace(K, V, V)}, which takes in an
    * instance of {@link Metadata} which can be used to provide metadata
    * information for the entry being stored, such as lifespan, version
    * of value...etc. The {@link Metadata} is only stored if the call is
    * successful.
    *
    * @param key key with which the specified value is associated
    * @param oldValue value expected to be associated with the specified key
    * @param newValue value to be associated with the specified key
    * @param metadata information to store alongside the new value
    * @return <tt>true</tt> if the value was replaced
    *
    * @since 5.3
    */
   boolean replace(K key, V oldValue, V newValue, Metadata metadata);

   /**
    * An overloaded form of {@link #putIfAbsent(K, V)}, which takes in an
    * instance of {@link Metadata} which can be used to provide metadata
    * information for the entry being stored, such as lifespan, version
    * of value...etc. The {@link Metadata} is only stored if the call is
    * successful.
    *
    * @param key key with which the specified value is to be associated
    * @param value value to be associated with the specified key
    * @param metadata information to store alongside the new value
    * @return the previous value associated with the specified key, or
    *         <tt>null</tt> if there was no mapping for the key.
    *
    * @since 5.3
    */
   V putIfAbsent(K key, V value, Metadata metadata);

   /**
    * Asynchronous version of {@link #put(Object, Object, Metadata)} which stores
    * metadata alongside the value.  This method does not block on remote calls,
    * even if your cache mode is synchronous.  Has no benefit over
    * {@link #put(Object, Object, Metadata)} if used in LOCAL mode.
    * <p/>
    *
    * @param key   key to use
    * @param value value to store
    * @param metadata information to store alongside the new value
    * @return a future containing the old value replaced.
    *
    * @since 5.3
    */
   NotifyingFuture<V> putAsync(K key, V value, Metadata metadata);

   // TODO: Even better: add replace/remove calls that apply the changes if a given function is successful
   // That way, you could do comparison not only on the cache value, but also based on version...etc

   /**
    * Retrieves a CacheEntry corresponding to a specific key.
    *
    * @param key the key whose associated cache entry is to be returned
    * @return the cache entry to which the specified key is mapped, or
    *         {@code null} if this map contains no mapping for the key
    *
    * @since 5.3
    */
   CacheEntry getCacheEntry(K key);

}
