package org.infinispan;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.BatchingCache;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.FilteringListenable;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableFunction;

/**
 * The central interface of Infinispan.  A Cache provides a highly concurrent, optionally distributed data structure
 * with additional features such as:
 * <ul> <li>JTA transaction compatibility</li> <li>Eviction support for evicting entries from memory to prevent {@link
 * OutOfMemoryError}s</li> <li>Persisting entries to a {@link org.infinispan.persistence.spi.CacheLoader}, either when they are evicted as an overflow,
 * or all the time, to maintain persistent copies that would withstand server failure or restarts.</li> </ul>
 * For convenience, Cache extends {@link ConcurrentMap} and implements all methods accordingly.  Methods like
 * {@link #keySet()}, {@link #values()} and {@link #entrySet()} produce backing collections in that updates done to them
 * also update the original Cache instance.  Certain methods on these maps can be expensive however (prohibitively so
 * when using a distributed cache).  The {@link #size()} and {@link #containsValue(Object)} methods upon invocation can
 * also be expensive just as well.  The reason these methods are expensive are that they take into account entries
 * stored in a configured {@link org.infinispan.persistence.spi.CacheLoader} and remote entries when using a distributed cache.
 * Frequent use of these methods is not recommended if used in this manner.  These aforementioned methods do take into
 * account in-flight transactions, however key/value pairs read in using an iterator will not be placed into the transactional
 * context to prevent {@link OutOfMemoryError}s.  Please note all of these methods behavior can be controlled using
 * a {@link org.infinispan.context.Flag} to disable certain things such as taking into account the loader.  Please see
 * each method on this interface for more details.
 * Also, like many {@link ConcurrentMap} implementations, Cache does not support the use of <code>null</code> keys or
 * values.
 * <h2>Asynchronous operations</h2>
 * Cache also supports the use of "async" remote operations.  Note that these methods
 * only really make sense if you are using a clustered cache.  I.e., when used in LOCAL mode, these "async" operations
 * offer no benefit whatsoever.  These methods, such as {@link #putAsync(Object, Object)} offer the best of both worlds
 * between a fully synchronous and a fully asynchronous cache in that a {@link java.util.concurrent.CompletableFuture} is returned.  The
 * <code>CompletableFuture</code> can then be ignored or thrown away for typical asynchronous behaviour, or queried for
 * synchronous behaviour, which would block until any remote calls complete.  Note that all remote calls are, as far as
 * the transport is concerned, synchronous.  This allows you the guarantees that remote calls succeed, while not
 * blocking your application thread unnecessarily.  For example, usage such as the following could benefit from the
 * async operations:
 * <pre>
 *   CompletableFuture f1 = cache.putAsync("key1", "value1");
 *   CompletableFuture f2 = cache.putAsync("key2", "value2");
 *   CompletableFuture f3 = cache.putAsync("key3", "value3");
 *   f1.get();
 *   f2.get();
 *   f3.get();
 * </pre>
 * The net result is behavior similar to synchronous RPC calls in that at the end, you have guarantees that all calls
 * completed successfully, but you have the added benefit that the three calls could happen in parallel.  This is
 * especially advantageous if the cache uses distribution and the three keys map to different cache instances in the
 * cluster.
 * Also, the use of async operations when within a transaction return your local value only, as expected.  A
 * CompletableFuture is still returned though for API consistency.
 * <h2>Constructing a Cache</h2>
 * An instance of the Cache is usually obtained by using a {@link org.infinispan.manager.CacheContainer}.
 * <pre>
 *   CacheManager cm = new DefaultCacheManager(); // optionally pass in a default configuration
 *   Cache c = cm.getCache();
 * </pre>
 * See the {@link org.infinispan.manager.CacheContainer} interface for more details on providing specific configurations, using multiple caches
 * in the same JVM, etc.
  * Please see the <a href="http://www.jboss.org/infinispan/docs">Infinispan documentation</a> and/or the <a
 * href="http://www.jboss.org/community/wiki/5minutetutorialonInfinispan">5 Minute Usage Tutorial</a> for more details.
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
public interface Cache<K, V> extends BasicCache<K, V>, BatchingCache, FilteringListenable<K, V> {
   /**
    * Under special operating behavior, associates the value with the specified key. <ul> <li> Only goes through if the
    * key specified does not exist; no-op otherwise (similar to {@link ConcurrentMap#putIfAbsent(Object, Object)})</li>
    * <li> Force asynchronous mode for replication to prevent any blocking.</li> <li> invalidation does not take place.
    * </li> <li> 0ms lock timeout to prevent any blocking here either. If the lock is not acquired, this method is a
    * no-op, and swallows the timeout exception.</li> <li> Ongoing transactions are suspended before this call, so
    * failures here will not affect any ongoing transactions.</li> <li> Errors and exceptions are 'silent' - logged at a
    * much lower level than normal, and this method does not throw exceptions</li> </ul> This method is for caching data
    * that has an external representation in storage, where, concurrent modification and transactions are not a
    * consideration, and failure to put the data in the cache should be treated as a 'suboptimal outcome' rather than a
    * 'failing outcome'.
    * An example of when this method is useful is when data is read from, for example, a legacy datastore, and is cached
    * before returning the data to the caller.  Subsequent calls would prefer to get the data from the cache and if the
    * data doesn't exist in the cache, fetch again from the legacy datastore.
    * See <a href="http://jira.jboss.com/jira/browse/JBCACHE-848">JBCACHE-848</a> for details around this feature.
    *
    * @param key   key with which the specified value is to be associated.
    * @param value value to be associated with the specified key.
    * @throws IllegalStateException if {@link #getStatus()} would not return {@link ComponentStatus#RUNNING}.
    */
   void putForExternalRead(K key, V value);

   /**
    * An overloaded form of {@link #putForExternalRead(K, V)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    *
    * @since 7.0
    */
   void putForExternalRead(K key, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #putForExternalRead(K, V)}, which takes in lifespan parameters.
    *
    * @param key          key to use
    * @param value        value to store
    * @param lifespan     lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit time unit for lifespan
    * @param maxIdle      the maximum amount of time this key is allowed to be idle for before it is considered as
    *                     expired
    * @param maxIdleUnit  time unit for max idle time
    * @since 7.0
    */
   void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Evicts an entry from the memory of the cache.  Note that the entry is <i>not</i> removed from any configured cache
    * stores or any other caches in the cluster (if used in a clustered mode).  Use {@link #remove(Object)} to remove an
    * entry from the entire cache system.
        * This method is designed to evict an entry from memory to free up memory used by the application.  This method uses
    * a 0 lock acquisition timeout so it does not block in attempting to acquire locks.  It behaves as a no-op if the
    * lock on the entry cannot be acquired <i>immediately</i>.
        * Important: this method should not be called from within a transaction scope.
    *
    * @param key key to evict
    */
   void evict(K key);

   org.infinispan.configuration.cache.Configuration getCacheConfiguration();

   /**
    * Retrieves the cache manager responsible for creating this cache instance.
    *
    * @return a cache manager
    */
   EmbeddedCacheManager getCacheManager();

   AdvancedCache<K, V> getAdvancedCache();

   ComponentStatus getStatus();

   /**
    * Returns a count of all elements in this cache and cache loader across the entire cluster.
    * Only a subset of entries is held in memory at a time when using a loader or remote entries, to prevent possible
    * memory issues, however the loading of said entries can still be very slow.
    * If there are performance concerns then the {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} flag should be used to
    * avoid hitting the cache loader in case if this is not needed in the size calculation.
    * Also, if you want the local contents only you can use the {@link org.infinispan.context.Flag#CACHE_MODE_LOCAL} flag so
    * that other remote nodes are not queried for data.  However, the loader will still be used unless the previously
    * mentioned {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} is also configured.
    * If this method is used in a transactional context, note this method will not bring additional values into the
    * transaction context and thus objects that haven't yet been read will act in a
    * {@link IsolationLevel#READ_COMMITTED} behavior irrespective of the configured
    * isolation level.  However, values that have been previously modified or read that are in the context will be
    * adhered to. e.g. any write modification or any previous read when using
    * {@link IsolationLevel#REPEATABLE_READ}
    * This method should only be used for debugging purposes such as to verify that the cache contains all the keys
    * entered. Any other use involving execution of this method on a production system is not recommended.
    *
    * @return the number of key-value mappings in this cache and cache loader across the entire cluster.
    */
   @Override
   int size();

   /**
    * Returns a set view of the keys contained in this cache and cache loader across the entire cluster.
    * Modifications and changes to the cache will be reflected in the set and vice versa. When this method is called
    * nothing is actually queried as the backing set is just returned.  Invocation on the set itself is when the
    * various operations are ran.
    * <h4>Unsupported Operations</h4>
    * Care should be taken when invoking {@link java.util.Set#toArray()}, {@link Set#toArray(Object[])},
    * {@link java.util.Set#size()}, {@link Set#retainAll(Collection)} and {@link java.util.Set#iterator()}
    * methods as they will traverse the entire contents of the cluster including a configured
    * {@link org.infinispan.persistence.spi.CacheLoader} and remote entries.  The former 2 methods especially have a
    * very high likelihood of causing a {@link java.lang.OutOfMemoryError} due to storing all the keys in the entire
    * cluster in the array.
    * Use involving execution of this method on a production system is not recommended as they can be quite expensive
    * operations
    * <h4>Supported Flags</h4>
    * Note any flag configured for the cache will also be passed along to the backing set when it was created.  If
    * additional flags are configured on the cache they will not affect any existing backings sets.
    * If there are performance concerns then the {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} flag should be used to
    * avoid hitting the cache store as this will cause all entries there to be read in (albeit in a batched form to
    * prevent {@link java.lang.OutOfMemoryError})
    * Also if you want the local contents only you can use the {@link org.infinispan.context.Flag#CACHE_MODE_LOCAL} flag so
    * that other remote nodes are not queried for data.  However, the loader will still be used unless the previously
    * mentioned {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} is also configured.
    * <h4>Iterator Use</h4>
    * This class implements the {@link CloseableIteratorSet} interface which creates a
    * {@link org.infinispan.commons.util.CloseableIterator} instead of a regular one.  This means this iterator must be
    * explicitly closed either through try with resource or calling the close method directly.  Technically this iterator
    * will also close itself if you iterate fully over it, but it is safest to always make sure you close it explicitly.
    * <h4>Unsupported Operations</h4>
    * Due to not being able to add null values the following methods are not supported and will throw
    * {@link java.lang.UnsupportedOperationException} if invoked.
    * {@link Set#add(Object)}
    * {@link Set#addAll(java.util.Collection)}
    *
    * @return a set view of the keys contained in this cache and cache loader across the entire cluster.
    */
   @Override
   CacheSet<K> keySet();

   /**
    * Returns a collection view of the values contained in this cache across the entire cluster. Modifications and
    * changes to the cache will be reflected in the set and vice versa. When this method is called nothing is actually
    * queried as the backing collection is just returned.  Invocation on the collection itself is when the various
    * operations are ran.
    * Care should be taken when invoking {@link Collection#toArray()}, {@link Collection#toArray(Object[])},
    * {@link Collection#size()}, {@link Collection#retainAll(Collection)} and {@link Collection#iterator()}
    * methods as they will traverse the entire contents of the cluster including a configured
    * {@link org.infinispan.persistence.spi.CacheLoader} and remote entries.  The former 2 methods especially have a
    * very high likelihood of causing a {@link java.lang.OutOfMemoryError} due to storing all the keys in the entire
    * cluster in the array.
    * Use involving execution of this method on a production system is not recommended as they can be quite expensive
    * operations
    * <h4>Supported Flags</h4>
    * Note any flag configured for the cache will also be passed along to the backing set when it was created.  If
    * additional flags are configured on the cache they will not affect any existing backings sets.
    * If there are performance concerns then the {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} flag should be used to
    * avoid hitting the cache store as this will cause all entries there to be read in (albeit in a batched form to
    * prevent {@link java.lang.OutOfMemoryError})
    * Also if you want the local contents only you can use the {@link org.infinispan.context.Flag#CACHE_MODE_LOCAL} flag so
    * that other remote nodes are not queried for data.  However the loader will still be used unless the previously
    * mentioned {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} is also configured.
    * <h4>Iterator Use</h4>
    * <p>This class implements the {@link CloseableIteratorCollection} interface which creates a
    * {@link org.infinispan.commons.util.CloseableIterator} instead of a regular one.  This means this iterator must be
    * explicitly closed either through try with resource or calling the close method directly.  Technically this iterator
    * will also close itself if you iterate fully over it, but it is safest to always make sure you close it explicitly.</p>
    * <p>The iterator retrieved using {@link CacheCollection#iterator()} supports the remove method, however the
    * iterator retrieved from {@link CacheStream#iterator()} will not support remove.</p>
    * <h4>Unsupported Operations</h4>
    * Due to not being able to add null values the following methods are not supported and will throw
    * {@link java.lang.UnsupportedOperationException} if invoked.
    * {@link Set#add(Object)}
    * {@link Set#addAll(java.util.Collection)}
    *
    * @return a collection view of the values contained in this cache and cache loader across the entire cluster.
    */
   @Override
   CacheCollection<V> values();

   /**
    * Returns a set view of the mappings contained in this cache and cache loader across the entire cluster.
    * Modifications and changes to the cache will be reflected in the set and vice versa. When this method is called
    * nothing is actually queried as the backing set is just returned.  Invocation on the set itself is when the
    * various operations are ran.
    * Care should be taken when invoking {@link java.util.Set#toArray()}, {@link Set#toArray(Object[])},
    * {@link java.util.Set#size()}, {@link Set#retainAll(Collection)} and {@link java.util.Set#iterator()}
    * methods as they will traverse the entire contents of the cluster including a configured
    * {@link org.infinispan.persistence.spi.CacheLoader} and remote entries.  The former 2 methods especially have a
    * very high likelihood of causing a {@link java.lang.OutOfMemoryError} due to storing all the keys in the entire
    * cluster in the array.
    * Use involving execution of this method on a production system is not recommended as they can be quite expensive
    * operations
    * <h4>Supported Flags</h4>
    * Note any flag configured for the cache will also be passed along to the backing set when it was created.  If
    * additional flags are configured on the cache they will not affect any existing backings sets.
    * If there are performance concerns then the {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} flag should be used to
    * avoid hitting the cache store as this will cause all entries there to be read in (albeit in a batched form to
    * prevent {@link java.lang.OutOfMemoryError})
    * Also if you want the local contents only you can use the {@link org.infinispan.context.Flag#CACHE_MODE_LOCAL} flag so
    * that other remote nodes are not queried for data.  However, the loader will still be used unless the previously
    * mentioned {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} is also configured.
    * <h4>Modifying or Adding Entries</h4>
    * An entry's value is supported to be modified by using the {@link java.util.Map.Entry#setValue(Object)} and it will update
    * the cache as well.  Also, this backing set does allow addition of a new Map.Entry(s) via the
    * {@link Set#add(Object)} or {@link Set#addAll(java.util.Collection)} methods.
    * <h4>Iterator Use</h4>
    * This class implements the {@link CloseableIteratorSet} interface which creates a
    * {@link org.infinispan.commons.util.CloseableIterator} instead of a regular one.  This means this iterator must be
    * explicitly closed either through try with resource or calling the close method directly.  Technically this iterator
    * will also close itself if you iterate fully over it, but it is safest to always make sure you close it explicitly.
    *
    * @return a set view of the mappings contained in this cache and cache loader across the entire cluster.
    */
   @Override
   CacheSet<Entry<K, V>> entrySet();

   /**
    * Removes all mappings from the cache.
        * <b>Note:</b> This should never be invoked in production unless you can guarantee no other invocations are ran
    * concurrently.
        * If the cache is transactional, it will not interact with the transaction.
    */
   @Override
   void clear();

   /**
    * Stops a cache. If the cache is clustered, this only stops the cache on the node where it is being invoked. If you
    * need to stop the cache across a cluster, use the {@link #shutdown()} method.
    */
   @Override
   void stop();

   /**
    * Performs a controlled, clustered shutdown of the cache. When invoked, the following operations are performed:
    * <ul>
    *    <li>rebalancing for the cache is disabled</li>
    *    <li>in-memory data is flushed/passivated to any persistent stores</li>
    *    <li>state is persisted to the location defined in {@link org.infinispan.configuration.global.GlobalStateConfigurationBuilder#persistentLocation(String)}</li>
    * </ul>
    *
    * This method differs from {@link #stop()} only in clustered modes,
    * and only when {@link org.infinispan.configuration.global.GlobalStateConfiguration#enabled()} is true, otherwise it
    * just behaves like {@link #stop()}.
    */
   default void shutdown() {
      stop();
   }

   /**
    * {@inheritDoc}
    * <p>
    * When this method is used on a clustered cache, either replicated or distributed, the function will be serialized
    * to owning nodes to perform the operation in the most performant way. However this means the function must be
    * marshallable either directly using the configured user marshaller, or by being {@link java.io.Serializable} itself.
    * <p>
    * For transactional caches, whenever the values of the caches are collections, and the mapping function modifies the collection, the collection
    * must be copied and not directly modified, otherwise whenever rollback is called it won't work.
    * This limitation could disappear in following releases if technically possible.
    */
   @Override
   V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction);

   /**
    * Overloaded {@link Cache#computeIfAbsent(Object, Function)} with Infinispan {@link SerializableFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    * @param key, the key to be computed
    * @param mappingFunction, mapping function to be appliyed to the key
    * @return the current (existing or computed) value associated with  the specified key, or null if the computed value is null
    */
   default V computeIfAbsent(K key, SerializableFunction<? super K, ? extends V> mappingFunction) {
      return computeIfAbsent(key, (Function<? super K, ? extends V>) mappingFunction);
   }

   /**
    * Overloaded {@link Cache#computeIfAbsent(Object, Function, long, TimeUnit)} with Infinispan {@link SerializableFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    * @param key, the key to be computed
    * @param mappingFunction, mapping function to be appliyed to the key
    * @return the current (existing or computed) value associated with  the specified key, or null if the computed value is null
    */
   default V computeIfAbsent(K key, SerializableFunction<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return computeIfAbsent(key, (Function<? super K, ? extends V>) mappingFunction, lifespan, lifespanUnit);
   }

   /**
    * Overloaded {@link Cache#computeIfAbsent(Object, Function, long, TimeUnit, long, TimeUnit)} with Infinispan {@link SerializableFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    * @param key, the key to be computed
    * @param mappingFunction, mapping function to be appliyed to the key
    * @return the current (existing or computed) value associated with  the specified key, or null if the computed value is null
    */
   default V computeIfAbsent(K key, SerializableFunction<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return computeIfAbsent(key, (Function<? super K, ? extends V>) mappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   /**
    * Overloaded {@link Cache#computeIfAbsentAsync(Object, Function)} with Infinispan {@link SerializableFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    * @param key, the key to be computed
    * @param mappingFunction, mapping function to be appliyed to the key
    * @return the current (existing or computed) value associated with  the specified key, or null if the computed value is null
    */
   default CompletableFuture<V> computeIfAbsentAsync(K key, SerializableFunction<? super K, ? extends V> mappingFunction) {
      return computeIfAbsentAsync(key, (Function<? super K, ? extends V>) mappingFunction);
   }

   /**
    * Overloaded {@link Cache#computeIfAbsentAsync(Object, Function, long, TimeUnit)} with Infinispan {@link SerializableFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    * @param key, the key to be computed
    * @param mappingFunction, mapping function to be appliyed to the key
    * @return the current (existing or computed) value associated with  the specified key, or null if the computed value is null
    */
   default CompletableFuture<V> computeIfAbsentAsync(K key, SerializableFunction<? super K, ? extends V> mappingFunction,
         long lifespan, TimeUnit lifespanUnit) {
      return computeIfAbsentAsync(key, (Function<? super K, ? extends V>) mappingFunction, lifespan, lifespanUnit);
   }

   /**
    * Overloaded {@link Cache#computeIfAbsentAsync(Object, Function, long, TimeUnit, long, TimeUnit)} with Infinispan {@link SerializableFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    * @param key, the key to be computed
    * @param mappingFunction, mapping function to be appliyed to the key
    * @return the current (existing or computed) value associated with  the specified key, or null if the computed value is null
    */
   default CompletableFuture<V> computeIfAbsentAsync(K key, SerializableFunction<? super K, ? extends V> mappingFunction,
         long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return computeIfAbsentAsync(key, (Function<? super K, ? extends V>) mappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   /**
    * {@inheritDoc}
    * <p>
    * When this method is used on a clustered cache, either replicated or distributed, the bifunction will be serialized
    * to owning nodes to perform the operation in the most performant way.  However this means the bifunction must be
    * marshallable either directly using the configured user marshaller, or by being {@link java.io.Serializable} itself.
    * <p>
    * For transactional caches, whenever the values of the caches are collections, and the mapping function modifies the collection, the collection
    * must be copied and not directly modified, otherwise whenever rollback is called it won't work.
    * This limitation could disappear in following releases if technically possible.
    */
   @Override
   V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

   /**
    * Overloaded {@link Cache#computeIfPresent(Object, BiFunction)} with Infinispan {@link SerializableBiFunction}
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    *
    * @param key, the key to be computed
    * @param remappingFunction, mapping function to be appliyed to the key
    * @return computed value or null if nothing is computed or computation result is null
    */
   default V computeIfPresent(K key,
                              SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return computeIfPresent(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction);
   }

   /**
    * Overloaded {@link Cache#computeIfPresentAsync(Object, BiFunction)} with Infinispan {@link SerializableBiFunction}
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    *
    * @param key, the key to be computed
    * @param remappingFunction, mapping function to be appliyed to the key
    * @return computed value or null if nothing is computed or computation result is null
    */
   default CompletableFuture<V> computeIfPresentAsync(K key,
         SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return computeIfPresentAsync(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction);
   }

   /**
    * {@inheritDoc}
    * <p>
    * When this method is used on a clustered cache, either replicated or distributed, the bifunction will be serialized
    * to owning nodes to perform the operation in the most performant way.  However this means the bifunction must be
    * marshallable either directly using the configured user marshaller, or by being {@link java.io.Serializable} itself.
    * <p>
    * For transactional caches, whenever the values of the caches are collections, and the mapping function modifies the collection, the collection
    * must be copied and not directly modified, otherwise whenever rollback is called it won't work.
    * This limitation could disappear in following releases if technically possible.
    */
   @Override
   V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

   /**
    * Overloaded {@link Cache#compute(Object, BiFunction)} with Infinispan {@link SerializableBiFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    *
    * @param key, the key to be computed
    * @param remappingFunction, mapping function to be appliyed to the key
    * @return computation result (can be null)
    */
   default V compute(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return compute(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction);
   }

   /**
    * Overloaded {@link Cache#compute(Object, BiFunction, long, TimeUnit)} with Infinispan {@link SerializableBiFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    *
    * @param key, the key to be computed
    * @param remappingFunction, mapping function to be appliyed to the key
    * @return computation result (can be null)
    */
   default V compute(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return compute(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction, lifespan, lifespanUnit);
   }

   /**
    * Overloaded {@link Cache#compute(Object, BiFunction, long, TimeUnit, long, TimeUnit)} with Infinispan {@link SerializableBiFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    *
    * @param key, the key to be computed
    * @param remappingFunction, mapping function to be appliyed to the key
    * @return computation result (can be null)
    */
   default V compute(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return compute(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   /**
    * Overloaded {@link Cache#computeAsync(Object, BiFunction)} with Infinispan {@link SerializableBiFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    *
    * @param key, the key to be computed
    * @param remappingFunction, mapping function to be appliyed to the key
    * @return computation result (can be null)
    */
   default CompletableFuture<V> computeAsync(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return computeAsync(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction);
   }

   /**
    * Overloaded {@link Cache#computeAsync(Object, BiFunction, long, TimeUnit)} with Infinispan {@link SerializableBiFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    *
    * @param key, the key to be computed
    * @param remappingFunction, mapping function to be appliyed to the key
    * @return computation result (can be null)
    */
   default CompletableFuture<V> computeAsync(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return computeAsync(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction, lifespan, lifespanUnit);
   }

   /**
    * Overloaded {@link Cache#computeAsync(Object, BiFunction, long, TimeUnit, long, TimeUnit)} with Infinispan {@link SerializableBiFunction}.
    *
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}
    *
    * @param key, the key to be computed
    * @param remappingFunction, mapping function to be appliyed to the key
    * @return computation result (can be null)
    */
   default CompletableFuture<V> computeAsync(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return computeAsync(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   /**
    * {@inheritDoc}
    * <p>
    * When this method is used on a clustered cache, either replicated or distributed, the bifunction will be serialized
    * to owning nodes to perform the operation in the most performant way.  However this means the bifunction must be
    * marshallable either directly using the configured user marshaller, or by being {@link java.io.Serializable} itself.
    * <p>
    * For transactional caches, whenever the values of the caches are collections, and the mapping function modifies the collection, the collection
    * must be copied and not directly modified, otherwise whenever rollback is called it won't work.
    * This limitation could disappear in following releases if technically possible.
    */
   @Override
   V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction);


   /**
    * Overloaded {@link Cache#merge(Object, Object, BiFunction)} with Infinispan {@link SerializableBiFunction}.
    * <p>
    * The compiler will pick this overload for lambda parameters, making them {@link java.io.Serializable}.
    *
    * @param key               key with which the resulting value is to be associated
    * @param value             the non-null value to be merged with the existing value associated with the key or, if no
    *                          existing value or a null value is associated with the key, to be associated with the key
    * @param remappingFunction the function to recompute a value if present
    */
   default V merge(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return merge(key, value, (BiFunction<? super V, ? super V, ? extends V>) remappingFunction);
   }

   /**
    * Overloaded {@link #merge(Object, Object, BiFunction, long, TimeUnit)} with Infinispan {@link SerializableBiFunction}.
    *
    * @param key                key to use
    * @param value              new value to merge with existing value
    * @param remappingFunction  function to use to merge new and existing values into a merged value to store under key
    * @param lifespan           lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit       time unit for lifespan
    * @return the merged value that was stored under key
    * @since 9.4
    */
   default V merge(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return merge(key, value, (BiFunction<? super V, ? super V, ? extends V>) remappingFunction, lifespan, lifespanUnit);
   }

   /**
    * Overloaded {@link #merge(Object, Object, BiFunction, long, TimeUnit, long, TimeUnit)} with Infinispan {@link SerializableBiFunction}.
    *
    * @param key                key to use
    * @param value              new value to merge with existing value
    * @param remappingFunction  function to use to merge new and existing values into a merged value to store under key
    * @param lifespan           lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit       time unit for lifespan
    * @param maxIdleTime        the maximum amount of time this key is allowed to be idle for before it is considered as
    *                           expired
    * @param maxIdleTimeUnit    time unit for max idle time
    * @return the merged value that was stored under key
    * @since 9.4
    */
   default V merge(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return merge(key, value, (BiFunction<? super V, ? super V, ? extends V>) remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   /**
    * Overloaded {@link #mergeAsync(Object, Object, BiFunction)} with Infinispan {@link SerializableBiFunction}.
    *
    * @param key                key to use
    * @param value              new value to merge with existing value
    * @param remappingFunction  function to use to merge new and existing values into a merged value to store under key
    * @return the merged value that was stored under key
    * @since 10.0
    */
   default CompletableFuture<V> mergeAsync(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return mergeAsync(key, value, (BiFunction<? super V, ? super V, ? extends V>) remappingFunction);
   }

   /**
    * Overloaded {@link #mergeAsync(Object, Object, BiFunction, long, TimeUnit)} with Infinispan {@link SerializableBiFunction}.
    *
    * @param key                key to use
    * @param value              new value to merge with existing value
    * @param remappingFunction  function to use to merge new and existing values into a merged value to store under key
    * @param lifespan           lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit       time unit for lifespan
    * @return the merged value that was stored under key
    * @since 10.0
    */
   default CompletableFuture<V> mergeAsync(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return mergeAsync(key, value, (BiFunction<? super V, ? super V, ? extends V>) remappingFunction, lifespan, lifespanUnit);
   }

   /**
    * Overloaded {@link #mergeAsync(Object, Object, BiFunction, long, TimeUnit, long, TimeUnit)} with Infinispan {@link SerializableBiFunction}.
    *
    * @param key                key to use
    * @param value              new value to merge with existing value
    * @param remappingFunction  function to use to merge new and existing values into a merged value to store under key
    * @param lifespan           lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit       time unit for lifespan
    * @param maxIdleTime        the maximum amount of time this key is allowed to be idle for before it is considered as
    *                           expired
    * @param maxIdleTimeUnit    time unit for max idle time
    * @return the merged value that was stored under key
    * @since 10.0
    */
   default CompletableFuture<V> mergeAsync(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return mergeAsync(key, value, (BiFunction<? super V, ? super V, ? extends V>) remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }
}
