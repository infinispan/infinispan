package org.infinispan.client.hotrod;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.TransactionalCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.query.dsl.Query;

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
 * <b>Note on default expiration values:</b> Due to limitations on the first
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
public interface RemoteCache<K, V> extends BasicCache<K, V>, TransactionalCache {
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
    * {@inheritDoc}
    * <p>
    * The returned value is only sent back if {@link Flag#FORCE_RETURN_VALUE} is enabled.
    */
   V remove(Object key);

   /**
    * {@inheritDoc}
    * <p>
    * This method requires 2 round trips to the server. The first to retrieve the value and version and a second to
    * remove the key with the version if the value matches. If possible user should use
    * {@link RemoteCache#getWithMetadata(Object)} and {@link RemoteCache#removeWithVersion(Object, long)}.
    */
   boolean remove(Object key, Object value);

   /**
    * {@inheritDoc}
    * <p>
    * This method requires 2 round trips to the server. The first to retrieve the value and version and a second to
    * replace the key with the version if the value matches. If possible user should use
    * {@link RemoteCache#getWithMetadata(Object)} and
    * {@link RemoteCache#replaceWithVersion(Object, Object, long)}.
    */
   @Override
   boolean replace(K key, V oldValue, V newValue);

   /**
    * {@inheritDoc}
    * <p>
    * This method requires 2 round trips to the server. The first to retrieve the value and version and a second to
    * replace the key with the version if the value matches. If possible user should use
    * {@link RemoteCache#getWithMetadata(Object)} and
    * {@link RemoteCache#replaceWithVersion(Object, Object, long, long, TimeUnit, long, TimeUnit)}.
    */
   @Override
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit);

   /**
    * {@inheritDoc}
    * <p>
    * This method requires 2 round trips to the server. The first to retrieve the value and version and a second to
    * replace the key with the version if the value matches. If possible user should use
    * {@link RemoteCache#getWithMetadata(Object)} and
    * {@link RemoteCache#replaceWithVersion(Object, Object, long, long, TimeUnit, long, TimeUnit)} if possible.
    */
   @Override
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * @see #remove(Object, Object)
    */
   CompletableFuture<Boolean> removeWithVersionAsync(K key, long version);

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
    * A overloaded form of {@link #replaceWithVersion(Object, Object, long)}
    * which takes in lifespan and maximum idle time parameters.
    *
    * @param key key to use
    * @param newValue new value to be associated with the key
    * @param version numeric version that should match the one in the server
    *                for the operation to succeed
    * @param lifespan lifespan of the entry
    * @param lifespanTimeUnit {@link java.util.concurrent.TimeUnit} for lifespan
    * @param maxIdle the maximum amount of time this key is allowed
    *                           to be idle for before it is considered as expired
    * @param maxIdleTimeUnit {@link java.util.concurrent.TimeUnit} for maxIdle
    * @return true if the value was replaced
    */
   boolean replaceWithVersion(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit);

   /**
    * @see #replaceWithVersion(Object, Object, long)
    */
   CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version);

   /**
    * @see #replaceWithVersion(Object, Object, long)
    */
   CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds);

   /**
    * @see #replaceWithVersion(Object, Object, long)
    */
   CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds, int maxIdleSeconds);

   /**
    * @see #retrieveEntries(String, Object[], java.util.Set, int)
    */
   CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Set<Integer> segments, int batchSize);

   /**
    * Retrieve entries from the server
    *
    * @param filterConverterFactory Factory name for the KeyValueFilterConverter or null for no filtering.
    * @param filterConverterParams  Parameters to the KeyValueFilterConverter
    * @param segments               The segments to iterate. If null all segments will be iterated. An empty set will filter out all entries.
    * @param batchSize              The number of entries transferred from the server at a time.
    * @return Iterator for the entries
    */
   CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize);

   /**
    * @see #retrieveEntries(String, Object[], java.util.Set, int)
    */
   CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, int batchSize);

   /**
    * Retrieve entries from the server matching a query.
    *
    * @param filterQuery {@link Query}
    * @param segments    The segments to iterate. If null all segments will be iterated. An empty set will filter out all entries.
    * @param batchSize   The number of entries transferred from the server at a time.
    * @return {@link CloseableIterator}
    */
   CloseableIterator<Entry<Object, Object>> retrieveEntriesByQuery(Query filterQuery, Set<Integer> segments, int batchSize);

   /**
    * Retrieve entries with metadata information
    */
   CloseableIterator<Entry<Object, MetadataValue<Object>>> retrieveEntriesWithMetadata(Set<Integer> segments, int batchSize);

   /**
    * Returns the {@link VersionedValue} associated to the supplied key param, or null if it doesn't exist.
    *
    * @deprecated Use {@link #getWithMetadata(Object)} instead
    */
   @Deprecated
   VersionedValue<V> getVersioned(K key);

   /**
    * Returns the {@link MetadataValue} associated to the supplied key param, or null if it doesn't exist.
    */
   MetadataValue<V> getWithMetadata(K key);

   /**
    * Asynchronously returns the {@link MetadataValue} associated to the supplied key param, or null if it doesn't exist.
    */
   CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key);

   /**
    * @inheritDoc
    * <p>
    * Due to this set being backed by the remote cache, each invocation on this set may require remote invocations
    * to retrieve or update the remote cache. The main benefit of this set being backed by the remote cache is that
    * this set internally does not require having to store any keys locally in memory and allows for the user to
    * iteratively retrieve keys from the cache which is more memory conservative.
    * <p>
    * If you do wish to create a copy of this set (requires all entries in memory), the user may invoke
    * <code>keySet().stream().collect(Collectors.toSet())</code> to copy the data locally. Then all operations on the
    * resulting set will not require remote access, however updates will not be reflected from the remote cache.
    * <p>
    * NOTE: this method returns a {@link CloseableIteratorSet} which requires the iterator, spliterator or stream
    * returned from it to be closed. Failure to do so may cause additional resources to not be freed.
    */
   @Override
   CloseableIteratorSet<K> keySet();

   /**
    * @inheritDoc
    * <p>
    * Due to this collection being backed by the remote cache, each invocation on this collection may require remote
    * invocations to retrieve or update the remote cache. The main benefit of this collection being backed by the remote
    * cache is that this collection internally does not require having to store any values locally in memory and allows
    * for the user to iteratively retrieve values from the cache which is more memory conservative.
    * <p>
    * If you do wish to create a copy of this collection (requires all entries in memory), the user may invoke
    * <code>values().stream().collect(Collectors.toList())</code> to copy the data locally. Then all operations on the
    * resulting list will not require remote access, however updates will not be reflected from the remote cache.
    * <p>
    * NOTE: this method returns a {@link CloseableIteratorCollection} which requires the iterator, spliterator or stream
    * returned from it to be closed. Failure to do so may cause additional resources to not be freed.
    */
   @Override
   CloseableIteratorCollection<V> values();

   /**
    * @inheritDoc
    * <p>
    * Due to this set being backed by the remote cache, each invocation on this set may require remote invocations
    * to retrieve or update the remote cache. The main benefit of this set being backed by the remote cache is that
    * this set internally does not require having to store any entries locally in memory and allows for the user to
    * iteratively retrieve entries from the cache which is more memory conservative.
    * <p>
    * The {@link CloseableIteratorSet#remove(Object)} method requires two round trips to the server to properly remove
    * an entry. This is because they first must retrieve the value and version
    * to see if it matches and if it does remove it using it's version.
    * <p>
    * If you do wish to create a copy of this set (requires all entries in memory), the user may invoke
    * <code>entrySet().stream().collect(Collectors.toSet())</code> to copy the data locally. Then all operations on the
    * resulting set will not require remote access, however updates will not be reflected from the remote cache.
    * <p>
    * NOTE: this method returns a {@link CloseableIteratorSet} which requires the iterator, spliterator or stream
    * returned from it to be closed. Failure to do so may cause additional resources to not be freed.
    */
   @Override
   CloseableIteratorSet<Entry<K, V>> entrySet();

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
   CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

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
    *
    * @deprecated Bulk retrievals can be quite expensive if for large data sets.
    * Alternatively, the different <code>retrieveEntries*</code> methods offer
    * lazy, pull-style, methods that retrieve bulk data more efficiently.
    */
   @Deprecated
   Map<K, V> getBulk();

   /**
    * Same as {@link #getBulk()}, but limits the returned set of values to the specified size. No ordering is guaranteed, and there is no
    * guarantee that "size" elements are returned( e.g. if the number of elements in the back-end server is smaller that "size")
    *
    * @deprecated Bulk retrievals can be quite expensive if for large data sets.
    * Alternatively, the different <code>retrieveEntries*</code> methods offer
    * lazy, pull-style, methods that retrieve bulk data more efficiently.
    */
   @Deprecated
   Map<K, V> getBulk(int size);

   /**
    * Retrieves all of the entries for the provided keys.  A key will not be present in
    * the resulting map if the entry was not found in the cache.
    * @param keys The keys to find values for
    * @return The entries that were present for the given keys
    */
   public Map<K, V> getAll(Set<? extends K> keys);

   /**
    * Returns the HotRod protocol version supported by this RemoteCache implementation
    */
   String getProtocolVersion();

   /**
    * Add a client listener to receive events that happen in the remote cache.
    * The listener object must be annotated with @{@link org.infinispan.client.hotrod.annotation.ClientListener} annotation.
    */
   void addClientListener(Object listener);

   /**
    * Add a client listener to receive events that happen in the remote cache.
    * The listener object must be annotated with @ClientListener annotation.
    */
   void addClientListener(Object listener, Object[] filterFactoryParams, Object[] converterFactoryParams);

   /**
    * Remove a previously added client listener. If the listener was not added
    * before, this operation is a no-op.
    */
   void removeClientListener(Object listener);

   /**
    * Returns a set with all the listeners registered by this client for the
    * given cache.
    */
   Set<Object> getListeners();

   /**
    * Executes a remote script passing a set of named parameters
    */
   <T> T execute(String scriptName, Map<String, ?> params);

   /**
    * Executes a remote script passing a set of named parameters, hinting that the script should be executed
    * on the server that is expected to store given key. The key itself is not transferred to the server.
    */
   default <T> T execute(String scriptName, Map<String, ?> params, Object key) {
      return execute(scriptName, params);
   }

   /**
    * Returns {@link CacheTopologyInfo} for this cache.
    */
   CacheTopologyInfo getCacheTopologyInfo();

   /**
    * Returns a cache where values are manipulated using {@link java.io.InputStream} and {@link java.io.OutputStream}
    */
   StreamingRemoteCache<K> streaming();

   /**
    * Return a new instance of {@link RemoteCache} using the supplied {@link DataFormat}.
    */
   <T, U> RemoteCache<T, U> withDataFormat(DataFormat dataFormat);

   /**
    * Return the currently {@link DataFormat} being used.
    */
   DataFormat getDataFormat();
}
