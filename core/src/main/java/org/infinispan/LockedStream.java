package org.infinispan;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.infinispan.configuration.cache.LockingConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializablePredicate;

/**
 * Stream that allows for operation upon data solely with side effects by using {@link LockedStream#forEach(BiConsumer)}
 * where the <b>BiConsumer</b> is invoked while guaranteeing that the entry being passed is properly locked for the
 * entire duration of the invocation.
 * <p>
 * An attempt is made to acquire the lock for an entry using the default
 * {@link LockingConfiguration#lockAcquisitionTimeout()} before invoking any operations on it.
 * </p>
 * @author wburns
 * @since 9.1
 */
public interface LockedStream<K, V> extends BaseCacheStream<CacheEntry<K, V>, LockedStream<K, V>> {
   /**
    * Returns a locked stream consisting of the elements of this stream that match
    * the given predicate.
    * <p>
    * This filter is after the lock is acquired for the given key. This way the filter will see the same value as
    * the consumer is given.
    * @param predicate predicate
    * @return a LockedStream with the filter applied
    */
   LockedStream<K, V> filter(Predicate<? super CacheEntry<K, V>> predicate);

   /**
    * Same as {@link LockedStream#filter(Predicate)} except that the Predicate must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate the predicate to filter out unwanted entries
    * @return a LockedStream with the filter applied
    */
   default LockedStream<K, V> filter(SerializablePredicate<? super CacheEntry<K ,V>> predicate) {
      return filter((Predicate<? super CacheEntry<K, V>> ) predicate);
   }

   /**
    * Performs an action for each element of this stream on the primary owner of the given key.
    * <p>
    * This method is performed while holding exclusive lock over the given entry and will be released
    * only after the consumer has completed. If the entry is directly modified via the
    * {@link java.util.Map.Entry#setValue(Object)} method this will be the same as if
    * {@link java.util.Map#put(Object, Object)} was invoked.
    * <p>
    * If using a pessimistic transaction this lock is not held using a transaction and thus the user can start a
    * transaction in this consumer which also must be completed before returning. A transaction can be started in
    * the consumer and if done it will share the same lock used to obtain the key.
    * <p>
    * Remember that if you are using an explicit transaction or an async method that these must be completed before
    * the consumer returns to guarantee that they are operating within the scope of the lock for the given key. Failure
    * to do so will lead into possible inconsistency as they will be performing operations without the proper locking.
    * <p>
    * Some methods on the provided cache may not work as expected. These include
    * {@link AdvancedCache#putForExternalRead(Object, Object)}, {@link AdvancedCache#lock(Object[])},
    * {@link AdvancedCache#lock(Collection)}, and {@link AdvancedCache#removeGroup(String)}.
    * If these methods are used inside of the Consumer on the cache it will throw a {@link IllegalStateException}.
    * This is due to possible interactions with transactions while using these commands.
    * @param biConsumer the biConsumer to run for each entry under their lock
    */
   void forEach(BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> biConsumer);

   /**
    * Same as {@link LockedStream#forEach(BiConsumer)}  except that the BiConsumer must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param biConsumer the biConsumer to run for each entry under their lock
    */
   default void forEach(SerializableBiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> biConsumer) {
      forEach((BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>>) biConsumer);
   }

   /**
    * Sets the timeout for the acquisition of the lock for each entry.
    * @param time the maximum time to wait
    * @param unit the time unit of the timeout argument
    * @return a LockedStream with the timeout applied
    */
   LockedStream timeout(long time, TimeUnit unit);

   /**
    * This method is not supported when using a {@link LockedStream}
    */
   @Override
   LockedStream segmentCompletionListener(SegmentCompletionListener listener) throws UnsupportedOperationException;

   /**
    * This method is not supported when using a {@link LockedStream}
    */
   @Override
   Iterator<CacheEntry<K, V>> iterator() throws UnsupportedOperationException;

   /**
    * This method is not supported when using a {@link LockedStream}
    */
   @Override
   Spliterator<CacheEntry<K, V>> spliterator() throws UnsupportedOperationException;
}
