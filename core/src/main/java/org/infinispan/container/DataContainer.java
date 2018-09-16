package org.infinispan.container;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;

/**
 * The main internal data structure which stores entries. Care should be taken when using this directly as entries
 * could be stored in a different way than they were given to a {@link org.infinispan.Cache}. If you wish to convert
 * entries to the stored format, you should use the provided {@link org.infinispan.encoding.DataConversion} such as
 * <pre>
 * cache.getAdvancedCache().getKeyDataConversion().toStorage(key);
 * </pre>
 * when dealing with keys or the following when dealing with values
 * <pre>
 * cache.getAdvancedCache().getValueDataConversion().toStorage(value);
 * </pre>
 * You can also convert from storage to the user provided type by using the
 * {@link org.infinispan.encoding.DataConversion#fromStorage(Object)} method on any value returned from the DataContainer
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
    * <p>
    * This method should be used instead of {@link #get(Object)}} when called while iterating through the data container
    * using methods like {@link #iterator()} to avoid changing the underlying collection's order.
    *
    * @param k key under which entry is stored
    * @return entry, if it exists, or null if not
    */
   InternalCacheEntry<K, V> peek(Object k);

   /**
    * Puts an entry in the cache along with metadata adding information such lifespan of entry, max idle time, version
    * information...etc.
    * <p>
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
    * <p>
    * The {@code key} must be activate by invoking {@link org.infinispan.eviction.ActivationManager#onRemove(Object,
    * boolean)}.
    *
    * @param k key to remove
    * @return entry removed, or null if it didn't exist or had expired
    */
   InternalCacheEntry<K, V> remove(Object k);

   /**
    * @return count of the number of entries in the container excluding expired entries
    * @implSpec
    * Default method invokes the {@link #iterator()} method and just counts entries.
    */
   default int size() {
      int size = 0;
      // We have to loop through to make sure to remove expired entries
      for (InternalCacheEntry<K, V> ignore : this) {
         if (++size == Integer.MAX_VALUE) return Integer.MAX_VALUE;
      }
      return size;
   }

   /**
    *
    * @return count of the number of entries in the container including expired entries
    */
   int sizeIncludingExpired();

   /**
    * Removes all entries in the container
    */
   @Stop(priority = 999)
   void clear();

   /**
    * Returns a set of keys in the container. When iterating through the container using this method, clients should
    * never call {@link #get(Object)} method but instead {@link #peek(Object)}, in order to avoid changing the order of
    * the underlying collection as a side of effect of iterating through it.
    * <p>
    * This set of keys will include expired entries. If you wish to only retrieve non expired keys please use the
    * {@link DataContainer#iterator()} method and retrieve keys from there.
    * @return a set of keys
    * @deprecated Please use iterator method if bulk operations are required.
    * @implSpec
    * Default implementation just throws a {@link UnsupportedOperationException}.
    */
   @Deprecated
   default Set<K> keySet() {
      throw new UnsupportedOperationException();
   }

   /**
    * This returns all values in the container including expired entries. If you wish to only receive values that
    * are not expired it is recommended to use {@link DataContainer#entrySet()} and pull values from there directly.
    * @return a set of values contained in the container
    * @deprecated Please use iterator method if bulk operations are required.
    * @implSpec
    * Default implementation just throws a {@link UnsupportedOperationException}.
    */
   @Deprecated
   default Collection<V> values() {
      throw new UnsupportedOperationException();
   }

   /**
    * Returns a mutable set of immutable cache entries exposed as immutable Map.Entry instances. Clients of this method
    * such as Cache.entrySet() operation implementors are free to convert the set into an immutable set if needed, which
    * is the most common use case.
    * <p>
    * If a client needs to iterate through a mutable set of mutable cache entries, it should iterate the container
    * itself rather than iterating through the return of entrySet().
    * <p>
    * This set is a read only backed view of the entries underneath. This set will only show non expired entries when
    * invoked. The size method of the set will count expired entries for the purpose of having a O(1) time cost compared
    * to O(N) if it is to not count expired entries.
    * @return a set of immutable cache entries
    * @deprecated Please use iterator method if bulk operations are required.
    * @implSpec
    * Default implementation just throws a {@link UnsupportedOperationException}.
    */
   @Deprecated
   default Set<InternalCacheEntry<K, V>> entrySet() {
      throw new UnsupportedOperationException();
   }

   /**
    * Atomically, it removes the key from {@code DataContainer} and passivates it to persistence.
    * <p>
    * The passivation must be done by invoking the method {@link org.infinispan.eviction.PassivationManager#passivate(org.infinispan.container.entries.InternalCacheEntry)}.
    *
    * @param key The key to evict.
    */
   void evict(K key);

   /**
    * Computes the new value for the key.
    * <p>
    * See {@link org.infinispan.container.DataContainer.ComputeAction#compute(Object,
    * org.infinispan.container.entries.InternalCacheEntry, InternalEntryFactory)}.
    * <p>
    * The {@code key} must be activate by invoking {@link org.infinispan.eviction.ActivationManager#onRemove(Object,
    * boolean)} or {@link org.infinispan.eviction.ActivationManager#onUpdate(Object, boolean)} depending if the value
    * returned by the {@link org.infinispan.container.DataContainer.ComputeAction} is null or not respectively.
    * <p>
    * Note the entry provided to {@link org.infinispan.container.DataContainer.ComputeAction} may be expired as these
    * entries are not filtered as many other methods do.
    * @param key    The key.
    * @param action The action that will compute the new value.
    * @return The {@link org.infinispan.container.entries.InternalCacheEntry} associated to the key.
    */
   InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action);

   /**
    * Executes task specified by the given action on the container key/values filtered using the specified key filter.
    * @implSpec
    * The default implementation is equivalent to
    * <pre> {@code
    * forEach(ice -> {
    *    if (filter == null || filter.accept(ice.getKey())) {
    *       action.accept(ice.getKey(), ice);
    *    }
    * }
    * }</pre>
    * @param filter the filter for the container keys
    * @param action the specified action to execute on filtered key/values
    * @throws InterruptedException
    * @deprecated since 9.3 Please use the {@link #iterator()} method and apply filtering manually
    */
   @Deprecated
   default void executeTask(final KeyFilter<? super K> filter, BiConsumer<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException {
      forEach(ice -> {
         if (filter == null || filter.accept(ice.getKey())) {
            action.accept(ice.getKey(), ice);
         }
      });
   }

   /**
    * Executes task specified by the given action on the container key/values filtered using the specified keyvalue filter.
    * @implSpec
    * The default implementation is equivalent to
    * <pre> {@code
    * iterator().forEachRemaining(ice -> {
    *    if (filter == null || filter.accept(ice.getKey(), ice.getValue(), ice.getMetadata())) {
    *       action.accept(ice.getKey(), ice);
    *    }
    * }
    * }</pre>
    * @param filter the filter for the container key/values
    * @param action the specified action to execute on filtered key/values
    * @throws InterruptedException
    * @deprecated since 9.3 Please use the {@link #iterator()} method and apply filtering manually
    */
   @Deprecated
   default void executeTask(KeyValueFilter<? super K, ? super V> filter, BiConsumer<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException {
      forEach(ice -> {
         if (filter == null || filter.accept(ice.getKey(), ice.getValue(), ice.getMetadata())) {
            action.accept(ice.getKey(), ice);
         }
      });
   }

   /**
    * {@inheritDoc}
    * <p>This iterator only returns entries that are not expired, however it will not remove them while doing so.</p>
    * @return iterator that doesn't produce expired entries
    */
   @Override
   Iterator<InternalCacheEntry<K, V>> iterator();

   /**
    * {@inheritDoc}
    * <p>This spliterator only returns entries that are not expired, however it will not remove them while doing so.</p>
    * @return spliterator that doesn't produce expired entries
    */
   @Override
   default Spliterator<InternalCacheEntry<K, V>> spliterator() {
      return Spliterators.spliterator(iterator(), sizeIncludingExpired(),
            Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
   }

   /**
    * Same as {@link DataContainer#iterator()} except that is also returns expired entries.
    * @return iterator that returns all entries including expired ones
    */
   Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired();

   /**
    * Same as {@link DataContainer#spliterator()} except that is also returns expired entries.
    * @return spliterator that returns all entries including expired ones
    */
   default Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired() {
      return Spliterators.spliterator(iteratorIncludingExpired(), sizeIncludingExpired(),
            Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
   }

   interface ComputeAction<K, V> {

      /**
       * Computes the new value for the key.
       *
       * @return The new {@code InternalCacheEntry} for the key, {@code null} if the entry is to be removed or {@code
       * oldEntry} is the entry is not to be changed (i.e. not entries are added, removed or touched).
       */
      InternalCacheEntry<K, V> compute(K key, InternalCacheEntry<K, V> oldEntry, InternalEntryFactory factory);

   }

   /**
    * Resizes the capacity of the underlying container. This is only supported if the container is bounded.
    * An {@link UnsupportedOperationException} is thrown otherwise.
    *
    * @param newSize the new size
    */
   default void resize(long newSize) {
      throw new UnsupportedOperationException();
   }

   /**
    * Returns the capacity of the underlying container. This is only supported if the container is bounded. An {@link UnsupportedOperationException} is thrown
    * otherwise.
    *
    * @return
    */
   default long capacity() {
      throw new UnsupportedOperationException();
   }

   /**
    * Returns how large the eviction size is currently. This is only supported if the container is bounded. An
    * {@link UnsupportedOperationException} is thrown otherwise. This value will always be lower than the value returned
    * from {@link DataContainer#capacity()}
    * @return how large the counted eviction is
    */
   default long evictionSize() {
      throw new UnsupportedOperationException();
   }
}
