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
public interface CacheWriter<K,V> extends Lifecycle {

   /**
    * Used to initialize a cache loader.  Typically invoked by the {@link org.infinispan.persistence.manager.PersistenceManager}
    * when setting up cache loaders.
    */
   void init(InitializationContext ctx);

   void write(MarshalledEntry<K,V> entry);

   boolean delete(K key);
}
