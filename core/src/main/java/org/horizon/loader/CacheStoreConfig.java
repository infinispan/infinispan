package org.horizon.loader;

import org.horizon.loader.decorators.SingletonStoreConfig;
import org.horizon.loader.decorators.AsyncStoreConfig;

/**
 * Defines config elements for all CacheStoreConfigs.
 *
 * @author Mircea.Markus@jboss.com
 */
public interface CacheStoreConfig extends CacheLoaderConfig, Cloneable {
   
   boolean isPurgeOnStartup();

   boolean isFetchPersistentState();

   void setFetchPersistentState(boolean fetchPersistentState);

   void setIgnoreModifications(boolean ignoreModifications);

   boolean isIgnoreModifications();

   void setPurgeOnStartup(boolean purgeOnStartup);

   SingletonStoreConfig getSingletonStoreConfig();

   void setSingletonStoreConfig(SingletonStoreConfig singletonStoreConfig);

   AsyncStoreConfig getAsyncStoreConfig();

   void setAsyncStoreConfig(AsyncStoreConfig asyncStoreConfig);

   public boolean isPurgeSynchronously();

   public void setPurgeSynchronously(boolean purgeSynchronously);
}
