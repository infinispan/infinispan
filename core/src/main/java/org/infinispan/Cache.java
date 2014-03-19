package org.infinispan;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.BatchingCache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.FilteringListenable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * The central interface of Infinispan.  A Cache provides a highly concurrent, optionally distributed data structure
 * with additional features such as:
 * <p/>
 * <ul> <li>JTA transaction compatibility</li> <li>Eviction support for evicting entries from memory to prevent {@link
 * OutOfMemoryError}s</li> <li>Persisting entries to a {@link org.infinispan.persistence.spi.CacheLoader}, either when they are evicted as an overflow,
 * or all the time, to maintain persistent copies that would withstand server failure or restarts.</li> </ul>
 * <p/>
 * <p/>
 * <p/>
 * For convenience, Cache extends {@link ConcurrentMap} and implements all methods accordingly, although methods like
 * {@link ConcurrentMap#keySet()}, {@link ConcurrentMap#values()} and {@link ConcurrentMap#entrySet()} are expensive
 * (prohibitively so when using a distributed cache) and frequent use of these methods is not recommended.
 * <p />
 * {@link #size()} provides the size of the local, internal data container only.  This does not take into account
 * in-fly transactions, entries stored in a cache store, or remote entries.  It may also take into consideration
 * entries that have expired but haven't yet been removed from the internal container, as well as entries in the L1
 * cache if L1 is enabled along with distribution as a clustering mode.  See the Infinispan User Guide section on
 * <a href="https://docs.jboss.org/author/display/ISPN51/Clustering+modes#Clusteringmodes-L1Caching">L1 caching</a> for more details.
 * <p/>
 * Also, like many {@link ConcurrentMap} implementations, Cache does not support the use of <tt>null</tt> keys or
 * values.
 * <p/>
 * <h3>Unsupported operations</h3>
 * <p>{@link #containsValue(Object)}</p>
 * <h3>Asynchronous operations</h3> Cache also supports the use of "async" remote operations.  Note that these methods
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
 * <h3>Constructing a Cache</h3> An instance of the Cache is usually obtained by using a {@link org.infinispan.manager.CacheContainer}.
 * <pre>
 *   CacheManager cm = new DefaultCacheManager(); // optionally pass in a default configuration
 *   Cache c = cm.getCache();
 * </pre>
 * See the {@link org.infinispan.manager.CacheContainer} interface for more details on providing specific configurations, using multiple caches
 * in the same JVM, etc.
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
public interface Cache<K, V> extends BasicCache<K, V>, BatchingCache, FilteringListenable<K, V> {
   /**
    * Under special operating behavior, associates the value with the specified key. <ul> <li> Only goes through if the
    * key specified does not exist; no-op otherwise (similar to {@link ConcurrentMap#putIfAbsent(Object, Object)})</i>
    * <li> Force asynchronous mode for replication to prevent any blocking.</li> <li> invalidation does not take place.
    * </li> <li> 0ms lock timeout to prevent any blocking here either. If the lock is not acquired, this method is a
    * no-op, and swallows the timeout exception.</li> <li> Ongoing transactions are suspended before this call, so
    * failures here will not affect any ongoing transactions.</li> <li> Errors and exceptions are 'silent' - logged at a
    * much lower level than normal, and this method does not throw exceptions</li> </ul> This method is for caching data
    * that has an external representation in storage, where, concurrent modification and transactions are not a
    * consideration, and failure to put the data in the cache should be treated as a 'suboptimal outcome' rather than a
    * 'failing outcome'.
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
    * @throws IllegalStateException if {@link #getStatus()} would not return {@link ComponentStatus#RUNNING}.
    */
   void putForExternalRead(K key, V value);

   /**
    * Evicts an entry from the memory of the cache.  Note that the entry is <i>not</i> removed from any configured cache
    * stores or any other caches in the cluster (if used in a clustered mode).  Use {@link #remove(Object)} to remove an
    * entry from the entire cache system.
    * <p/>
    * This method is designed to evict an entry from memory to free up memory used by the application.  This method uses
    * a 0 lock acquisition timeout so it does not block in attempting to acquire locks.  It behaves as a no-op if the
    * lock on the entry cannot be acquired <i>immediately</i>.
    * <p/>
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
    * Returns a count of all elements in this cache and cache loader.  To avoid performance issues, there will be no
    * attempt to count keys from other nodes.
    * <p/>
    * If there are memory concerns then the {@link org.infinispan.context.Flag.SKIP_CACHE_LOAD} flag should be used to
    * avoid hitting the cache store as all local keys will be loaded into memory at once.
    * <p/>
    * This method should only be used for debugging purposes such as to verify that the cache contains all the keys
    * entered. Any other use involving execution of this method on a production system is not recommended.
    * <p/>
    *
    * @return the number of key-value mappings in this cache and cache loader
    */
   @Override
   int size();

   /**
    * Returns a set view of the keys contained in this cache and cache loader. This set is immutable, so it cannot be
    * modified and changes to the cache won't be reflected in the set. When this method is called on a cache configured
    * with distribution mode, the set returned only contains the keys locally available in the cache instance including
    * the cache loader if provided. To avoid memory issues, there will be not attempt to bring keys from other nodes.
    * <p/>
    * If there are memory concerns then the {@link org.infinispan.context.Flag.SKIP_CACHE_LOAD} flag should be used to
    * avoid hitting the cache store as all local keys will be in memory at once.
    * <p/>
    * This method should only be used for debugging purposes such as to verify that the cache contains all the keys
    * entered. Any other use involving execution of this method on a production system is not recommended.
    * <p/>
    *
    * @return a set view of the keys contained in this cache and cache loader.
    */
   @Override
   Set<K> keySet();

   /**
    * Returns a collection view of the values contained in this cache. This collection is immutable, so it cannot be modified
    * and changes to the cache won't be reflected in the set. When this method is called on a cache configured with
    * distribution mode, the collection returned only contains the values locally available in the cache instance
    * including the cache loader if provided. To avoid memory issues, there is no attempt to bring values from other nodes.
    * <p/>
    * If there are memory concerns then the {@link org.infinispan.context.Flag.SKIP_CACHE_LOAD} flag should be used to
    * avoid hitting the cache store as all local values will be in memory at once.
    * <p/>
    * This method should only be used for testing or debugging purposes such as to verify that the cache contains all the
    * values entered. Any other use involving execution of this method on a production system is not recommended.
    * <p/>
    *
    * @return a collection view of the values contained in this cache and cache loader.
    */
   @Override
   Collection<V> values();

   /**
    * Returns a set view of the mappings contained in this cache and cache loader. This set is immutable, so it cannot
    * be modified and changes to the cache won't be reflected in the set. Besides, each element in the returned set is
    * an immutable {@link Map.Entry}. When this method is called on a cache configured with distribution mode, the set
    * returned only contains the mappings locally available in the cache instance. To avoid memory issues, there will
    * be not attempt to bring mappings from other nodes.
    * <p/>
    * If there are memory concerns then the {@link org.infinispan.context.Flag.SKIP_CACHE_LOAD} flag should be used to
    * avoid hitting the cache store as all local entries will be in memory at once.
    * <p/>
    * This method should only be used for debugging purposes such as to verify that the cache contains all the mappings
    * entered. Any other use involving execution of this method on a production system is not recommended.
    * <p/>
    *
    * @return a set view of the mappings contained in this cache and cache loader
    */
   @Override
   Set<Map.Entry<K, V>> entrySet();
}
