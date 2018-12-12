package org.infinispan.persistence.spi;

import java.util.concurrent.Executor;

import org.infinispan.commons.util.Experimental;
import org.infinispan.marshall.core.MarshalledEntry;

/**
 * Defines functionality for advanced expiration techniques.  Note this interface allows for providing not just the key
 * when an entry is expired.  This is important so that proper cluster wide expiration can be performed.
 * @since 8.0
 */
@Experimental
public interface AdvancedCacheExpirationWriter<K, V> extends AdvancedCacheWriter<K, V> {
   /**
    * Using the thread in the pool, removed all the expired data from the persistence storage. For each removed entry,
    * the supplied listener is invoked.  This should be preferred to
    * {@link AdvancedCacheWriter#purge(Executor, PurgeListener)} since it allows for value and metadata to be provided
    * which provides more accurate expiration when coordination is required.
    *
    * @param executor the executor to invoke the given command on
    * @param listener the listener that is notified for each expired entry
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   void purge(Executor executor, ExpirationPurgeListener<K, V> listener);

   /**
    * Callback to be notified when an entry is removed by the {@link #purge(Executor, ExpirationPurgeListener)} method.
    * Note this interface adds a new method to the purge listener.  It is possible that a cache store may want to
    * have a key only expiration and a key/metadata for various performance reasons.
    */
   interface ExpirationPurgeListener<K, V> extends PurgeListener<K> {

      /**
       * Optional. If possible, {@link AdvancedCacheExpirationWriter} implementors should invoke this method for every
       * entry that is purged from the store. One of the side effects of not implementing this method is that listeners
       * do not receive {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired} for the
       * entries that are removed from the persistent store directly.
       */
      default void marshalledEntryPurged(MarshallableEntry<K, V> entry) {
         marshalledEntryPurged(MarshalledEntry.wrap(entry));
      }

      /**
       * @deprecated since 10.0, use {{@link #marshalledEntryPurged(MarshallableEntry)}} instead.
       */
      @Deprecated
      default void marshalledEntryPurged(MarshalledEntry<K, V> entry) {
         // no-op
      }
   }

   /**
    * This method is never called. Implementers of {@link AdvancedCacheExpirationWriter} must instead
    * implement {@link #purge(Executor, ExpirationPurgeListener)}.
    */
   @Override
   default void purge(Executor threadPool, PurgeListener<? super K> listener) {
      throw new UnsupportedOperationException();
   }
}
