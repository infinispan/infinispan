package org.infinispan.loaders;

import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;

/**
 * Defines config elements for all CacheStoreConfigs.
 *
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
 */
public interface CacheStoreConfig extends CacheLoaderConfig, Cloneable {

   Boolean isPurgeOnStartup();

   Boolean isFetchPersistentState();

   void setFetchPersistentState(Boolean fetchPersistentState);

   void setIgnoreModifications(Boolean ignoreModifications);
   
   CacheStoreConfig fetchPersistentState(Boolean fetchPersistentState);

   CacheStoreConfig ignoreModifications(Boolean ignoreModifications);

   Boolean isIgnoreModifications();

   void setPurgeOnStartup(Boolean purgeOnStartup);
   
   CacheStoreConfig purgeOnStartup(Boolean purgeOnStartup);

   SingletonStoreConfig getSingletonStoreConfig();

   void setSingletonStoreConfig(SingletonStoreConfig singletonStoreConfig);
   
   AsyncStoreConfig getAsyncStoreConfig();

   void setAsyncStoreConfig(AsyncStoreConfig asyncStoreConfig);
   
   public Boolean isPurgeSynchronously();

   void setPurgeSynchronously(Boolean purgeSynchronously);
   
   CacheStoreConfig purgeSynchronously(Boolean purgeSynchronously);
   
   AsyncStoreConfig configureAsyncStore();

   SingletonStoreConfig configureSingletonStore();
   
}
