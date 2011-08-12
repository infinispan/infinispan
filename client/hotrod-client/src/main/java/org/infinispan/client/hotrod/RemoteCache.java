/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.client.hotrod;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Provides remote reference to a Hot Rod server/cluster. It implements {@link org.infinispan.Cache}, but given its
 * nature (remote) some operations are not supported. All these unsupported operations are being overridden within this
 * interface and documented as such.
 * <p/>
 * <b>New operations</b>: besides the operations inherited from {@link org.infinispan.Cache}, RemoteCache also adds new
 * operations to optimize/reduce network traffic: e.g. versioned put operation.
 * <p/>
 * <b>Concurrency</b>: implementors of this interface will support multi-threaded access, similar to the way {@link
 * org.infinispan.Cache} supports it.
 * <p/>
 * <b>Return values</b>: previously existing values for certain {@link java.util.Map} operations are not returned, null
 * is returned instead. E.g. {@link java.util.Map#put(Object, Object)} returns the previous value associated to the
 * supplied key. In case of RemoteCache, this returns null.
 * <p/>
 * <b>Synthetic operations</b>: aggregate operations are being implemented based on other Hot Rod operations. E.g. all
 * the {@link java.util.Map#putAll(java.util.Map)} is implemented through multiple individual puts. This means that the
 * these operations are not atomic and that they are costly, e.g. as the number of network round-trips is not one, but
 * the size of the added map. All these synthetic operations are documented as such.
 * <p/>
 * <b>changing default behavior through {@link org.infinispan.client.hotrod.Flag}s</b>: it is possible to change the
 * default cache behaviour by using flags on an per invocation basis. E.g.
 * <pre>
 *      RemoteCache cache = getRemoteCache();
 *      Object oldValue = cache.withFlags(Flag.FORCE_RETURN_VALUE).put(aKey, aValue);
 * </pre>
 * In the previous example, using {@link org.infinispan.client.hotrod.Flag#FORCE_RETURN_VALUE} will make the client to
 * also return previously existing value associated with <tt>aKey</tt>. If this flag would not be present, Infinispan
 * would return (by default) <tt>null</tt>. This is in order to avoid fetching a possibly large object from the remote
 * server, which might not be needed. The flags as set by the {@link org.infinispan.client.hotrod.RemoteCache#withFlags(Flag...)}
 * operation only apply for the very next operation executed <b>by the same thread</b> on the RemoteCache.
 * <p/>
 * <b><a href="http://community.jboss.org/wiki/Eviction">Eviction and expiration</a></b>: Unlike local {@link
 * org.infinispan.Cache} cache, which allows specifying time values with any granularity (as defined by {@link
 * TimeUnit}), HotRod only supports seconds as time units. If a different time unit is used instead, HotRod will
 * transparently convert it to seconds, using {@link java.util.concurrent.TimeUnit#toSeconds(long)} method. This might
 * result in loss of precision for values specified as nanos or milliseconds. <br/> Another fundamental difference is in
 * the case of lifespan (naturally does NOT apply for max idle): If number of seconds is bigger than 30 days, this
 * number of seconds is treated as UNIX time and so, represents the number of seconds since 1/1/1970. <br/>
 *
 * <b>Note on default expiration values:<b/> Due to limitations on the first
 * version of the protocol, it's not possible for clients to rely on default
 * lifespan and maxIdle values set on the server. This is because the protocol
 * does not support a way to tell the server that no expiration lifespan and/or
 * maxIdle were provided and that default values should be used. This will be
 * resolved in a future revision of the protocol. In the mean time, the
 * workaround is to explicitly provide the desired expiry lifespan/maxIdle
 * values in each remote cache operation.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface RemoteCache<K, V> extends Cache<K, V> {

   /**
    * Removes the given entry only if its version matches the supplied version. A typical use case looks like this:
    * <pre>
    * VersionedEntry ve = remoteCache.getVersioned(key);
    * //some processing
    * remoteCache.removeWithVersion(key, ve.getVersion();
    * </pre>
    * Lat call (removeWithVersion) will make sure that the entry will only be removed if it hasn't been changed in
    * between.
    *
    * @return true if the entry has been removed
    * @see VersionedValue
    * @see #getVersioned(Object)
    */
   boolean removeWithVersion(K key, long version);

   /**
    * @see #remove(Object, Object)
    */
   NotifyingFuture<Boolean> removeWithVersionAsync(K key, long version);

   /**
    * Replaces the given value only if its version matches the supplied version.
    * See {@link #removeWithVersion(Object, long)} for a sample usage of the
    * version-based methods.
    *
    * @param version numeric version that should match the one in the server
    *                for the operation to succeed
    * @return true if the value has been replaced
    * @see #getVersioned(Object)
    * @see VersionedValue
    */
   boolean replaceWithVersion(K key, V newValue, long version);

   /**
    * A overloaded form of {@link #replaceWithVersion(Object, Object, long)}
    * which takes in lifespan parameters.
    *
    * @param key key to use
    * @param newValue new value to be associated with the key
    * @param version numeric version that should match the one in the server
    *                for the operation to succeed
    * @param lifespanSeconds lifespan of the entry
    * @return true if the value was replaced
    */
   boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds);

   /**
    * A overloaded form of {@link #replaceWithVersion(Object, Object, long)}
    * which takes in lifespan and maximum idle time parameters.
    *
    * @param key key to use
    * @param newValue new value to be associated with the key
    * @param version numeric version that should match the one in the server
    *                for the operation to succeed
    * @param lifespanSeconds lifespan of the entry
    * @param maxIdleTimeSeconds the maximum amount of time this key is allowed
    *                           to be idle for before it is considered as expired
    * @return true if the value was replaced
    */
   boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds, int maxIdleTimeSeconds);

   /**
    * @see #replaceWithVersion(Object, Object, long)
    */
   NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version);

   /**
    * @see #replaceWithVersion(Object, Object, long)
    */
   NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds);

   /**
    * @see #replaceWithVersion(Object, Object, long)
    */
   NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds, int maxIdleSeconds);


   /**
    * Returns the {@link VersionedValue} associated to the supplied key param, or null if it doesn't exist.
    */
   VersionedValue getVersioned(K key);


   /**
    * Operation might be supported for smart clients that will be able to register for topology changes.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   void addListener(Object listener);

   /**
    * @throws UnsupportedOperationException
    * @see #addListener(Object)
    */
   @Override
   void removeListener(Object listener);

   /**
    * @throws UnsupportedOperationException
    * @see #addListener(Object)
    */
   @Override
   Set<Object> getListeners();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   int size();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   boolean isEmpty();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   boolean containsValue(Object value);

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   Set<K> keySet();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   Collection<V> values();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   Set<Entry<K, V>> entrySet();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   void evict(K key);

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   Configuration getConfiguration();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   boolean startBatch();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   void endBatch(boolean successful);

   /**
    * This operation is not supported. Consider using {@link #removeWithVersion(Object, long)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   boolean remove(Object key, Object value);

   /**
    * This operation is not supported. Consider using {@link #removeWithVersionAsync(Object, long)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   NotifyingFuture<Boolean> removeAsync(Object key, Object value);

   /**
    * This operation is not supported. Consider using {@link #replaceWithVersion(Object, Object, long)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   boolean replace(K key, V oldValue, V newValue);

   /**
    * This operation is not supported. Consider using {@link #replaceWithVersion(Object, Object, long, int)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit);

   /**
    * This operation is not supported. Consider using {@link #replaceWithVersion(Object, Object, long, int, int)}  instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * This operation is not supported. Consider using {@link #replaceWithVersionAsync(Object, Object, long)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue);

   /**
    * This operation is not supported. Consider using {@link #replaceWithVersion(Object, Object, long, int)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit);

   /**
    * This operation is not supported. Consider using {@link #replaceWithVersion(Object, Object, long, int, int)}  instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * This operation is not supported.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   AdvancedCache<K, V> getAdvancedCache();

   /**
    * This operation is not supported.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   void compact();

   /**
    * This operation is not supported.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   void putForExternalRead(K key, V value);

   /**
    * Synthetic operation. The client iterates over the set of keys and calls put for each one of them. This results in
    * operation not being atomic (if a failure happens after few puts it is not rolled back) and costly (for each key in
    * the parameter map a remote call is performed).
    */
   @Override
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   void putAll(Map<? extends K, ? extends V> m);

   ServerStatistics stats();

   /**
    * Applies one or more {@link Flag}s to the scope of a single invocation.  See the {@link Flag} enumeration to for
    * information on available flags.
    * <p />
    * Sample usage:
    * <pre>
    *    remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).put("hello", "world");
    * </pre>
    * @param flags
    * @return the current RemoteCache instance to continue running operations on.
    */
   RemoteCache<K, V> withFlags(Flag... flags);

   /**
    * Returns the {@link org.infinispan.client.hotrod.RemoteCacheManager} that created this cache.
    */
   RemoteCacheManager getRemoteCacheManager();

   /**
    * Bulk get operations, returns all the entries within the remote cache.
    *
    * @return the returned values depend on the configuration of the back-end infinispan servers. Read <a
    *         href="http://community.jboss.org/wiki/HotRodBulkGet-Design#Server_side">this</a> for more details. The
    *         returned Map is unmodifiable.
    */
   Map<K, V> getBulk();

   /**
    * Same as {@link #getBulk()}, but limits the returned set of values to the specified size. No ordering is guaranteed, and there is no
    * guarantee that "size" elements are returned( e.g. if the number of elements in the back-end server is smaller that "size")
    */
   Map<K, V> getBulk(int size);
}
