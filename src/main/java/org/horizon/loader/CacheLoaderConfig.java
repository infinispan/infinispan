package org.horizon.loader;

import org.horizon.loader.decorators.AsyncStoreConfig;
import org.horizon.loader.decorators.SingletonStoreConfig;

/**
 * Configures individual cache loaders
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheLoaderConfig extends Cloneable {
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

   CacheLoaderConfig clone();

   String getCacheLoaderClassName();

   void setCacheLoaderClassName(String s);
}
