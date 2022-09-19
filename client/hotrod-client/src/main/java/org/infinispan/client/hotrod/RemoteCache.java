package org.infinispan.client.hotrod;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.jmx.RemoteCacheClientStatisticsMXBean;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.TransactionalCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.IntSet;
import org.infinispan.query.dsl.Query;
import org.reactivestreams.Publisher;

/**
 * Provides remote reference to a Hot Rod server/cluster. It implements {@link BasicCache}, but given its
 * nature (remote) some operations are not supported. All these unsupported operations are being overridden within this
 * interface and documented as such.
 * <p/>
 * <b>New operations</b>: besides the operations inherited from {@link BasicCache}, RemoteCache also adds new
 * operations to optimize/reduce network traffic: e.g. versioned put operation.
 * <p/>
 * <b>Concurrency</b>: implementors of this interface will support multi-threaded access, similar to the way {@link
 * BasicCache} supports it.
 * <p/>
 * <b>Return values</b>: previously existing values for certain {@link java.util.Map} operations are not returned, null
 * is returned instead. E.g. {@link java.util.Map#put(Object, Object)} returns the previous value associated to the
 * supplied key. In case of RemoteCache, this returns null.
 * <p/>
 * <b>Changing default behavior through {@link org.infinispan.client.hotrod.Flag}s</b>: it is possible to change the
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
    * @see #getWithMetadata(Object)
    */
   boolean removeWithVersion(K key, long version);

   /**
    * {@inheritDoc}
    * <p>
    * The returned value is only sent back if {@link Flag#FORCE_RETURN_VALUE} is enabled.
    */
   @Override
   V remove(Object key);

   /**
    * {@inheritDoc}
    * <p>
    * This method requires 2 round trips to the server. The first to retrieve the value and version and a second to
    * remove the key with the version if the value matches. If possible user should use
    * {@link RemoteCache#getWithMetadata(Object)} and {@link RemoteCache#removeWithVersion(Object, long)}.
    */
   @Override
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
    * @see #getWithMetadata(Object)
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
    * @see #replaceWithVersion(Object, Object, long)
    */
   CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, long lifespanSeconds, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit);

   /**
    * @see #retrieveEntries(String, Object[], java.util.Set, int)
    */
   default CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Set<Integer> segments, int batchSize) {
      return retrieveEntries(filterConverterFactory, null, segments, batchSize);
   }

   /**
    * Retrieve entries from the server.
    *
    * @param filterConverterFactory Factory name for the KeyValueFilterConverter or null for no filtering.
    * @param filterConverterParams  Parameters to the KeyValueFilterConverter
    * @param segments               The segments to iterate. If null all segments will be iterated. An empty set will filter out all entries.
    * @param batchSize              The number of entries transferred from the server at a time.
    * @return Iterator for the entries
    */
   CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize);

   /**
    * Publishes the entries from the server in a non blocking fashion.
    * <p>
    * Any subscriber that subscribes to the returned Publisher must not block. It is therefore recommended to offload
    * any blocking or long running operations to a different thread and not use the invoking one. Failure to do so
    * may cause concurrent operations to stall.
    * @param filterConverterFactory Factory name for the KeyValueFilterConverter or null for no filtering.
    * @param filterConverterParams  Parameters to the KeyValueFilterConverter
    * @param segments               The segments to utilize. If null all segments will be utilized. An empty set will filter out all entries.
    * @param batchSize              The number of entries transferred from the server at a time.
    * @return Publisher for the entries
    */
   <E> Publisher<Entry<K, E>> publishEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize);

   /**
    * @see #retrieveEntries(String, Object[], java.util.Set, int)
    */
   default CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, int batchSize) {
      return retrieveEntries(filterConverterFactory, null, null, batchSize);
   }

   /**
    * Retrieve entries from the server matching a query.
    *
    * @param filterQuery {@link Query}
    * @param segments    The segments to iterate. If null all segments will be iterated. An empty set will filter out all entries.
    * @param batchSize   The number of entries transferred from the server at a time.
    * @return {@link CloseableIterator}
    */
   CloseableIterator<Entry<Object, Object>> retrieveEntriesByQuery(Query<?> filterQuery, Set<Integer> segments, int batchSize);

   /**
    * Publish entries from the server matching a query.
    * <p>
    * Any subscriber that subscribes to the returned Publisher must not block. It is therefore recommended to offload
    * any blocking or long running operations to a different thread and not use the invoking one. Failure to do so
    * may cause concurrent operations to stall.
    * @param filterQuery {@link Query}
    * @param segments    The segments to utilize. If null all segments will be utilized. An empty set will filter out all entries.
    * @param batchSize   The number of entries transferred from the server at a time.
    * @return Publisher containing matching entries
    */
   <E> Publisher<Entry<K, E>> publishEntriesByQuery(Query<?> filterQuery, Set<Integer> segments, int batchSize);

   /**
    * Retrieve entries with metadata information
    */
   CloseableIterator<Entry<Object, MetadataValue<Object>>> retrieveEntriesWithMetadata(Set<Integer> segments, int batchSize);

   /**
    * Publish entries with metadata information
    * <p>
    * Any subscriber that subscribes to the returned Publisher must not block. It is therefore recommended to offload
    * any blocking or long running operations to a different thread and not use the invoking one. Failure to do so
    * may cause concurrent operations to stall.
    * @param segments    The segments to utilize. If null all segments will be utilized. An empty set will filter out all entries.
    * @param batchSize   The number of entries transferred from the server at a time.
    * @return Publisher containing entries along with metadata
    */
   Publisher<Entry<K, MetadataValue<V>>> publishEntriesWithMetadata(Set<Integer> segments, int batchSize);

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
   default CloseableIteratorSet<K> keySet() {
      return keySet(null);
   }

   /**
    * This method is identical to {@link #keySet()} except that it will only return keys that map to the given segments.
    * Note that these segments will be determined by the remote server. Thus you should be aware of how many segments
    * it has configured and hashing algorithm it is using. If the segments and hashing algorithm are not the same
    * this method may return unexpected keys.
    * @param segments the segments of keys to return - null means all available
    * @return set containing keys that map to the given segments
    * @see #keySet()
    * @since 9.4
    */
   CloseableIteratorSet<K> keySet(IntSet segments);

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
   default CloseableIteratorCollection<V> values() {
      return values(null);
   }

   /**
    * This method is identical to {@link #values()} except that it will only return values that map to the given segments.
    * Note that these segments will be determined by the remote server. Thus you should be aware of how many segments
    * it has configured and hashing algorithm it is using. If the segments and hashing algorithm are not the same
    * this method may return unexpected values.
    * @param segments the segments of values to return - null means all available
    * @return collection containing values that map to the given segments
    * @see #values()
    * @since 9.4
    */
   CloseableIteratorCollection<V> values(IntSet segments);

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
   default CloseableIteratorSet<Entry<K, V>> entrySet() {
      return entrySet(null);
   }

   /**
    * This method is identical to {@link #entrySet()} except that it will only return entries that map to the given segments.
    * Note that these segments will be determined by the remote server. Thus you should be aware of how many segments
    * it has configured and hashing algorithm it is using. If the segments and hashing algorithm are not the same
    * this method may return unexpected entries.
    * @param segments the segments of entries to return - null means all available
    * @return set containing entries that map to the given segments
    * @see #entrySet()
    * @since 9.4
    */
   CloseableIteratorSet<Entry<K, V>> entrySet(IntSet segments);

   /**
    * Adds or overrides each specified entry in the remote cache. This operation provides better performance than calling put() for each entry.
    */
   @Override
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit);

   /**
    * Adds or overrides each specified entry in the remote cache. This operation provides better performance than calling put() for each entry.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * Adds or overrides each specified entry in the remote cache. This operation provides better performance than calling put() for each entry.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data);

   /**
    * Adds or overrides each specified entry in the remote cache. This operation provides better performance than calling put() for each entry.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit);

   /**
    * Adds or overrides each specified entry in the remote cache. This operation provides better performance than calling put() for each entry.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Adds or overrides each specified entry in the remote cache. This operation provides better performance than calling put() for each entry.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   void putAll(Map<? extends K, ? extends V> m);

   /**
    * Returns server-side statistics for this cache.
    * @deprecated use {@link #serverStatistics()} instead
    */
   @Deprecated
   default ServerStatistics stats() {
      return serverStatistics();
   }

   /**
    * Returns client-side statistics for this cache.
    */
   RemoteCacheClientStatisticsMXBean clientStatistics();

   /**
    * Returns server-side statistics for this cache.
    */
   ServerStatistics serverStatistics();

   /**
    * Returns server-side statistics for this cache.
    */
   CompletionStage<ServerStatistics> serverStatisticsAsync();

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
    * Returns the {@link org.infinispan.client.hotrod.RemoteCacheContainer} that created this cache.
    */
   RemoteCacheContainer getRemoteCacheContainer();

   /**
    * Returns the {@link org.infinispan.client.hotrod.RemoteCacheManager} that created this cache.
    * @deprecated Since 14.0. Use {@link #getRemoteCacheContainer()} instead.
    */
   @Deprecated
   default RemoteCacheManager getRemoteCacheManager() {
      return (RemoteCacheManager) this.getRemoteCacheContainer();
   }

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
    *
    * @deprecated Since 10.0, with no replacement
    */
   @Deprecated
   Set<Object> getListeners();

   /**
    * Executes a remote task without passing any parameters
    */
   default <T> T execute(String taskName) {
      return execute(taskName, Collections.emptyMap());
   }

   /**
    * Executes a remote task passing a set of named parameters
    */
   <T> T execute(String taskName, Map<String, ?> params);

   /**
    * Executes a remote task passing a set of named parameters, hinting that the task should be executed
    * on the server that is expected to store given key. The key itself is not transferred to the server.
    */
   default <T> T execute(String taskName, Map<String, ?> params, Object key) {
      return execute(taskName, params);
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

   /**
    * @return {@code true} if the cache can participate in a transaction, {@code false} otherwise.
    */
   boolean isTransactional();
}
