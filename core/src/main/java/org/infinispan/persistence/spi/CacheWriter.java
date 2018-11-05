package org.infinispan.persistence.spi;

import org.infinispan.commons.api.Lifecycle;

import net.jcip.annotations.ThreadSafe;

/**
 * Allows persisting data to an external storage, as opposed to the {@link CacheLoader}.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public interface CacheWriter<K, V> extends Lifecycle {

   /**
    * Used to initialize a cache loader.  Typically invoked by the {@link org.infinispan.persistence.manager.PersistenceManager}
    * when setting up cache loaders.
    *
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   void init(InitializationContext ctx);

   /**
    * Persists the entry to the storage.
    *
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    * @see MarshalledEntry
    */
   void write(MarshalledEntry<? extends K, ? extends V> entry);

   /**
    * @return true if the entry existed in the persistent store and it was deleted.
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   boolean delete(Object key);

   /**
    * Persist all provided entries to the store in a single batch update. If this is not supported by the
    * underlying store, then entries are written to the store individually via {@link #write(MarshalledEntry)}.
    *
    * @param entries an Iterable of MarshalledEntry to be written to the store.
    * @throws NullPointerException if entries is null.
    */
   default void writeBatch(Iterable<MarshalledEntry<? extends K, ? extends V>> entries) {
      entries.forEach(this::write);
   }

   /**
    * Remove all provided keys from the store in a single batch operation. If this is not supported by the
    * underlying store, then keys are removed from the store individually via {@link #delete(Object)}.
    *
    * @param keys an Iterable of entry Keys to be removed from the store.
    * @throws NullPointerException if keys is null.
    */
   default void deleteBatch(Iterable<Object> keys) {
      keys.forEach(this::delete);
   }

   /**
    * @return true if the writer can be connected to, otherwise false
    */
   default boolean isAvailable() {
      return true;
   }
}
