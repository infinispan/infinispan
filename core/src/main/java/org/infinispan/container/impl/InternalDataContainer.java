package org.infinispan.container.impl;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * Interface describing methods of a data container where operations can be indexed by the segment of the key
 * stored in the map. This allows for much more efficient iteration when only a subset of segments are required for
 * a given operation (which is the case very often with Distributed caches).
 * <p>
 * This container has a notion of what segments are currently associated with it and these can be controlled via
 * the {@link #removeSegments(IntSet)} and {@link #addSegments(IntSet)} methods. A segment can be added multiple times
 * and the implementation must be able to handle this. If a write occurs on a segment that is not associated with this
 * container it may ignore the write or it could store it temporarily if needed (additional caching). When segments
 * are removed, an implementation is free to remove any entries that map to segments that aren't associated to this
 * container.
 * @author wburns
 * @since 9.3
 */
public interface InternalDataContainer<K, V> extends DataContainer<K, V> {

   /**
    * Same as {@link DataContainer#get(Object)} except that the segment of the key can provided to lookup entries
    * without calculating the segment for the given key
    * @param segment segment for the key
    * @param k key under which entry is stored
    * @return entry, if it exists and has not expired, or null if not
    */
   InternalCacheEntry<K, V> get(int segment, Object k);

   /**
    * Same as {@link DataContainer#peek(Object)} except that the segment of the key can provided to lookup entries
    * without calculating the segment for the given key
    * @param segment segment for the key
    * @param k key under which entry is stored
    * @return entry, if it exists, or null if not
    */
   InternalCacheEntry<K, V> peek(int segment, Object k);

   /**
    * Same as {@link DataContainer#put(Object, Object, Metadata)} except that the segment of the key can provided to
    * write/lookup entries without calculating the segment for the given key.
    *
    * <p>Note: The timestamps ignored if the entry already exists in the data container.</p>
    *
    * @param segment segment for the key
    * @param k key under which to store entry
    * @param v value to store
    * @param metadata metadata of the entry
    * @param createdTimestamp creation timestamp, or {@code -1} to use the current time
    * @param lastUseTimestamp last use timestamp, or {@code -1} to use the current time
    *
    * @since 10.0
    */
   void put(int segment, K k, V v, Metadata metadata, long createdTimestamp, long lastUseTimestamp);

   /**
    * Same as {@link DataContainer#containsKey(Object)}  except that the segment of the key can provided to
    * lookup if the entry exists without calculating the segment for the given key.
    * @param segment segment for the key
    * @param k key under which entry is stored
    * @return true if entry exists and has not expired; false otherwise
    */
   boolean containsKey(int segment, Object k);

   /**
    * Same as {@link DataContainer#remove(Object)}  except that the segment of the key can provided to
    * remove the entry without calculating the segment for the given key.
    * @param segment segment for the key
    * @param k key to remove
    * @return entry removed, or null if it didn't exist or had expired
    */
   InternalCacheEntry<K, V> remove(int segment, Object k);

   /**
    * Same as {@link DataContainer#evict(Object)} except that the segment of the key can provided to
    * remove the entry without calculating the segment for the given key.
    * @param segment segment for the key
    * @param key The key to evict.
    */
   void evict(int segment, K key);

   /**
    * Same as {@link DataContainer#compute(Object, ComputeAction)}  except that the segment of the key can provided to
    * update entries without calculating the segment for the given key.
    * @param segment segment for the key
    * @param key    The key.
    * @param action The action that will compute the new value.
    * @return The {@link org.infinispan.container.entries.InternalCacheEntry} associated to the key.
    */
   InternalCacheEntry<K, V> compute(int segment, K key, ComputeAction<K, V> action);

   /**
    * Returns how many entries are present in the data container that map to the given segments without counting entries
    * that are currently expired.
    * @param segments segments of entries to count
    * @return count of the number of entries in the container excluding expired entries
    * @implSpec
    * Default method invokes the {@link #iterator(IntSet)} method and just counts entries.
    */
   default int size(IntSet segments) {
      int size = 0;
      // We have to loop through and count the entries
      for (Iterator<InternalCacheEntry<K, V>> iter = iterator(segments); iter.hasNext(); ) {
         iter.next();
         if (++size == Integer.MAX_VALUE) return Integer.MAX_VALUE;
      }
      return size;
   }

   /**
    * Returns how many entries are present in the data container that map to the given segments including any entries
    * that may be expired
    * @param segments segments of entries to count
    * @return count of the number of entries in the container including expired entries
    */
   default int sizeIncludingExpired(IntSet segments) {
      int size = 0;
      // We have to loop through and count the expired entries
      for (Iterator<InternalCacheEntry<K, V>> iter = iteratorIncludingExpired(segments); iter.hasNext(); ) {
         iter.next();
         if (++size == Integer.MAX_VALUE) return Integer.MAX_VALUE;
      }
      return size;
   }

   /**
    * Removes entries from the container whose key maps to one of the provided segments
    * @param segments segments of entries to remove
    */
   void clear(IntSet segments);

   /**
    * Same as {@link DataContainer#spliterator()} except that only entries that map to the provided segments are
    * returned via this spliterator. The spliterator will not return expired entries.
    * @param segments segments of entries to return
    * @return spliterator containing entries mapping to those segments that aren't expired
    */
   Spliterator<InternalCacheEntry<K, V>> spliterator(IntSet segments);

   /**
    * Same as {@link DataContainer#spliteratorIncludingExpired()} except that only entries that map to the provided
    * segments are returned via this spliterator. The spliterator will return expired entries as well.
    * @param segments segments of entries to use
    * @return spliterator containing entries mapping to those segments that could be expired
    */
   Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments);

   /**
    * Same as {@link DataContainer#iterator()} except that only entries that map to the provided segments are
    * returned via the iterator. The iterator will not return expired entries.
    * @param segments segments of entries to use
    * @return iterator that returns all entries mapped to the given segments
    */
   Iterator<InternalCacheEntry<K, V>> iterator(IntSet segments);

   /**
    * Same as {@link DataContainer#iteratorIncludingExpired()} except that only entries that map to the provided
    * segments are returned via the iterator. The iterator can return expired entries.
    * @param segments segments of entries to use
    * @return iterator that returns all entries mapped to the given segments that could be expired
    */
   Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments);

   /**
    * Performs the given action for each element of the container that maps to the given set of segments
    * until all elements have been processed or the action throws an exception.  Unless otherwise specified by the
    * implementing class, actions are performed in the order of iteration (if an iteration order is specified).
    * Exceptions thrown by the action are relayed to the caller.
    *
    * @implSpec
    * <p>The default implementation behaves as if:
    * <pre>{@code
    *   for (Iterator<InternalCacheEntry<K, V>> iter = iterator(segments) ; iter.hasNext() ; ) {
    *      InternalCacheEntry<K, V> ice = iter.next();
    *      action.accept(ice);
    *   }
    * }</pre>
    *
    * @param action The action to be performed for each element
    * @throws NullPointerException if the specified action is null
    */
   default void forEach(IntSet segments, Consumer<? super InternalCacheEntry<K, V>> action) {
      for (Iterator<InternalCacheEntry<K, V>> iter = iterator(segments) ; iter.hasNext() ; ) {
         InternalCacheEntry<K, V> ice = iter.next();
         action.accept(ice);
      }
   }

   /**
    * Performs the given action for each element of the container, even if expired. This method should be preferred when
    * user wants to perform an operation across all entries that depends on the segment it maps to.
    * @param action The action to be performed for each element
    * @throws NullPointerException if the specified action is null
    */
   void forEachIncludingExpired(ObjIntConsumer<? super InternalCacheEntry<K, V>> action);

   /**
    * Sets what segments this data container should be using. Already associated segments are unaffected by this and
    * takes a union of existing and new segments.
    * @param segments segments to associate with this container
    */
   void addSegments(IntSet segments);

   /**
    * Removes and un-associates the given segments. This will notify any listeners registered via
    * {@link #addRemovalListener(Consumer)} of entries that were removed due to no longer being associated with this
    * container. There is no guarantee if the consumer is invoked once or multiple times for a given group of segments
    * and could be in any order.
    * <p>
    * When this method is invoked an implementation is free to remove any entries that don't map to segments currently
    * associated with this container. Note that entries that were removed due to their segments never being associated
    * with this container do not notify listeners registered via {@link #addRemovalListener(Consumer)}.
    * @param segments segments that should no longer be associated with this container
    */
   void removeSegments(IntSet segments);

   /**
    * Adds a listener that is invoked whenever {@link #removeSegments(IntSet)} is invoked providing a way for
    * the listener to see what actual entries were removed from the container.
    * @param listener listener that invoked of removed entries
    */
   void addRemovalListener(Consumer<Iterable<InternalCacheEntry<K, V>>> listener);

   /**
    * Removes a previously registered listener via {@link #addRemovalListener(Consumer)}.
    * @param listener the listener to remove
    */
   void removeRemovalListener(Object listener);
}
