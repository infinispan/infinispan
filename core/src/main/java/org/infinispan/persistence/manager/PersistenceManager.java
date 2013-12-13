package org.infinispan.persistence.manager;

import java.util.Collection;
import java.util.Set;

import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;

/**
 * Defines the logic for interacting with the chain of external storage.
 *
 * @author Manik Surtani
 * @author Mircea Markus
 * @since 4.0
 */
public interface PersistenceManager extends Lifecycle {

   /**
    * Loads the data from the external store into memory during cache startup.
    */
   public void preload();

   /**
    * Marks the given storage as disabled.
    */
   void disableStore(String storeType);

   <T> Set<T> getStores(Class<T> storeClass);

   Collection<String> getStoresAsString();

   /**
    * Removes the expired entries from all the existing storage.
    */
   void purgeExpired();

   /**
    * Invokes {@link org.infinispan.persistence.spi.AdvancedCacheWriter#clear()} on all the stores that aloes it.
    */
   void clearAllStores(boolean skipSharedStores);

   boolean deleteFromAllStores(Object key, boolean skipSharedStores);

   void processOnAllStores(AdvancedCacheLoader.KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata);

   boolean activate(Object key);

   MarshalledEntry loadFromAllStores(Object key, InvocationContext context);

   void writeToAllStores(MarshalledEntry marshalledEntry, boolean skipSharedStores);

   /**
    * Returns the store one configured with fetch persistent state, or null if none exist.
    */
   AdvancedCacheLoader getStateTransferProvider();

   int size();
}


