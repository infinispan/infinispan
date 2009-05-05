package org.infinispan.loaders;

import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;

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
