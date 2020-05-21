package org.infinispan.persistence.spi;

import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import net.jcip.annotations.ThreadSafe;

/**
 * A specialised extension of the {@link CacheLoader} interface that allows processing parallel iteration over the
 * existing entries.
 *
 * @author Mircea Markus
 * @since 6.0
 * @deprecated since 11.0 replaced by {@link NonBlockingStore}
 */
@ThreadSafe
@Deprecated
public interface AdvancedCacheLoader<K, V> extends CacheLoader<K, V> {

   /**
    * Returns the number of elements in the store.
    *
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   int size();

   /**
    * Publishes all the keys from this store. The given publisher can be used by as many
    * {@link org.reactivestreams.Subscriber}s as desired. Keys are not retrieved until a given Subscriber requests
    * them from the {@link org.reactivestreams.Subscription}.
    * <p>
    * Stores will return only non expired keys
    * @param filter a filter - null is treated as allowing all entries
    * @return a publisher that will provide the keys from the store
    */
   default Publisher<K> publishKeys(Predicate<? super K> filter) {
      return Flowable.fromPublisher(entryPublisher(filter, false, true)).map(MarshallableEntry::getKey);
   }

   /**
    * Publishes all entries from this store.  The given publisher can be used by as many
    * {@link org.reactivestreams.Subscriber}s as desired. Entries are not retrieved until a given Subscriber requests
    * them from the {@link org.reactivestreams.Subscription}.
    * <p>
    * If <b>fetchMetadata</b> is true this store must guarantee to not return any expired entries.
    * @param filter a filter - null is treated as allowing all entries
    * @param fetchValue    whether or not to fetch the value from the persistent store. E.g. if the iteration is
    *                      intended only over the key set, no point fetching the values from the persistent store as
    *                      well
    * @param fetchMetadata whether or not to fetch the metadata from the persistent store. E.g. if the iteration is
    *                      intended only ove the key set, then no point fetching the metadata from the persistent store
    *                      as well
    * @return a publisher that will provide the entries from the store
    */
   Publisher<MarshallableEntry<K, V>> entryPublisher(Predicate<? super K> filter, boolean fetchValue,
                                                             boolean fetchMetadata);
}
