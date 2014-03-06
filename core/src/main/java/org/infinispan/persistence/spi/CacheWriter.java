package org.infinispan.persistence.spi;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.marshall.core.MarshalledEntry;

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
}
