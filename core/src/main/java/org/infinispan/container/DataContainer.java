package org.infinispan.container;

import java.util.Collection;
import java.util.Set;

import org.infinispan.commons.util.concurrent.ParallelIterableMap.KeyValueAction;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;

/**
 * The main internal data structure which stores entries
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DataContainer<K, V> extends Iterable<InternalCacheEntry<K, V>> {

   /**
    * Retrieves a cached entry
    *
    * @param k key under which entry is stored
    * @return entry, if it exists and has not expired, or null if not
    */
   InternalCacheEntry<K, V> get(Object k);

   /**
    * Retrieves a cache entry in the same way as {@link #get(Object)}} except that it does not update or reorder any of
    * the internal constructs. I.e., expiration does not happen, and in the case of the LRU container, the entry is not
    * moved to the end of the chain.
    * <p/>
    * This method should be used instead of {@link #get(Object)}} when called while iterating through the data container
    * using methods like {@link #keySet()} to avoid changing the underlying collection's order.
    *
    * @param k key under which entry is stored
    * @return entry, if it exists, or null if not
    */
   InternalCacheEntry<K, V> peek(Object k);

   /**
    * Puts an entry in the cache along with metadata adding information such lifespan of entry, max idle time, version
    * information...etc.
    * <p/>
    * The {@code key} must be activate by invoking {@link org.infinispan.eviction.ActivationManager#onUpdate(Object,
    * boolean)}.
    *
    * @param k key under which to store entry
    * @param v value to store
    * @param metadata metadata of the entry
    */
   void put(K k, V v, Metadata metadata);

   /**
    * Tests whether an entry exists in the container
    *
    * @param k key to test
    * @return true if entry exists and has not expired; false otherwise
    */
   boolean containsKey(Object k);

   /**
    * Removes an entry from the cache
    * <p/>
    * The {@code key} must be activate by invoking {@link org.infinispan.eviction.ActivationManager#onRemove(Object,
    * boolean)}.
    *
    * @param k key to remove
    * @return entry removed, or null if it didn't exist or had expired
    */
   InternalCacheEntry<K, V> remove(Object k);

   /**
    *
    * @return count of the number of entries in the container
    */
   int size();

   /**
    * Removes all entries in the container
    */
   @Stop(priority = 999)
   void clear();

   /**
    * Returns a set of keys in the container. When iterating through the container using this method, clients should
    * never call {@link #get(Object)} method but instead {@link #peek(Object)}, in order to avoid changing the order of
    * the underlying collection as a side of effect of iterating through it.
    *
    * @return a set of keys
    */
   Set<K> keySet();

   /**
    * @return a set of values contained in the container
    */
   Collection<V> values();

   /**
    * Returns a mutable set of immutable cache entries exposed as immutable Map.Entry instances. Clients of this method
    * such as Cache.entrySet() operation implementors are free to convert the set into an immutable set if needed, which
    * is the most common use case.
    * <p/>
    * If a client needs to iterate through a mutable set of mutable cache entries, it should iterate the container
    * itself rather than iterating through the return of entrySet().
    *
    * @return a set of immutable cache entries
    */
   Set<InternalCacheEntry<K, V>> entrySet();

   /**
    * Purges entries that have passed their expiry time
    */
   void purgeExpired();

   /**
    * Atomically, it removes the key from {@code DataContainer} and passivates it to persistence.
    * <p/>
    * The passivation must be done by invoking the method {@link org.infinispan.eviction.PassivationManager#passivate(org.infinispan.container.entries.InternalCacheEntry)}.
    *
    * @param key The key to evict.
    */
   void evict(K key);

   /**
    * Computes the new value for the key.
    * <p/>
    * See {@link org.infinispan.container.DataContainer.ComputeAction#compute(Object,
    * org.infinispan.container.entries.InternalCacheEntry, InternalEntryFactory)}.
    * <p/>
    * The {@code key} must be activate by invoking {@link org.infinispan.eviction.ActivationManager#onRemove(Object,
    * boolean)} or {@link org.infinispan.eviction.ActivationManager#onUpdate(Object, boolean)} depending if the value
    * returned by the {@link org.infinispan.container.DataContainer.ComputeAction} is null or not respectively.
    *
    * @param key    The key.
    * @param action The action that will compute the new value.
    * @return The {@link org.infinispan.container.entries.InternalCacheEntry} associated to the key.
    */
   InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action);

   /**
    * Executes task specified by the given action on the container key/values filtered using the specified key filter.
    *
    * @param filter the filter for the container keys
    * @param action the specified action to execute on filtered key/values
    * @throws InterruptedException
    */
   public void executeTask(final KeyFilter<? super K> filter, KeyValueAction<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException;

   /**
    * Executes task specified by the given action on the container key/values filtered using the specified keyvalue filter.
    *
    * @param filter the filter for the container key/values
    * @param action the specified action to execute on filtered key/values
    * @throws InterruptedException
    */
   public void executeTask(KeyValueFilter<? super K, ? super V> filter, KeyValueAction<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException;

   public static interface ComputeAction<K, V> {

      /**
       * Computes the new value for the key.
       *
       * @return The new {@code InternalCacheEntry} for the key, {@code null} if the entry is to be removed or {@code
       * oldEntry} is the entry is not to be changed (i.e. not entries are added, removed or touched).
       */
      InternalCacheEntry<K, V> compute(K key, InternalCacheEntry<K, V> oldEntry, InternalEntryFactory factory);

   }
}
