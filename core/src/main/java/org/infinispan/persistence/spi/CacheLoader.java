package org.infinispan.persistence.spi;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.marshall.core.MarshalledEntry;

/**
 * Defines the logic for loading data from an external storage. The writing of data is optional and coordinated through
 * a {@link CacheWriter}.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public interface CacheLoader<K, V> extends Lifecycle {

   /**
    * Used to initialize a cache loader.  Typically invoked by the {@link org.infinispan.persistence.manager.PersistenceManager}
    * when setting up cache loaders.
    *
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   void init(InitializationContext ctx);

   /**
    * Fetches an entry from the storage. If a {@link MarshalledEntry} needs to be created here, {@link
    * org.infinispan.persistence.spi.InitializationContext#getMarshalledEntryFactory()} and {@link
    * InitializationContext#getByteBufferFactory()} should be used.
    *
    * @return the entry, or null if the entry does not exist
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   MarshalledEntry<K, V> load(K key);

   /**
    * Returns true if the storage contains an entry associated with the given key.
    *
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   boolean contains(K key);
}
