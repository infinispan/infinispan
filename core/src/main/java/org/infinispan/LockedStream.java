package org.infinispan;

import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializablePredicate;

/**
 * Stream that allows for operation upon data solely with side effects by using {@link LockedStream#forEach(BiConsumer)}
 * where the <b>BiConsumer</b> is invoked while guaranteeing that the entry being passed is properly locked for the
 * entire duration of the invocation.
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
    * @return
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
    * Performs an action for each element of this stream.
    * <p>
    * This method is performed while holding exclusive lock over the given entry and will be released
    * only after the consumer has completed.
    * <p>
    * If using a pessimistic transaction this lock is not held using a transaction and thus the user can start a
    * transaction in this consumer which also must be completed before returning.
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
    * This method is not supported when using a {@link LockedStream}
    */
   @Override
   BaseCacheStream segmentCompletionListener(SegmentCompletionListener listener) throws UnsupportedOperationException;
}
