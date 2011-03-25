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

   /**
    * @deprecated use {@link #fetchPersistentState(Boolean)} instead
    */
   @Deprecated
   void setFetchPersistentState(Boolean fetchPersistentState);

   /**
    * @deprecated use {@link #ignoreModifications(Boolean)} instead
    */
   @Deprecated
   void setIgnoreModifications(Boolean ignoreModifications);
   
   CacheStoreConfig fetchPersistentState(Boolean fetchPersistentState);

   CacheStoreConfig ignoreModifications(Boolean ignoreModifications);

   Boolean isIgnoreModifications();

   /**
    * @deprecated use {@link #purgeOnStartup(Boolean)} instead
    */
   @Deprecated
   void setPurgeOnStartup(Boolean purgeOnStartup);
   
   CacheStoreConfig purgeOnStartup(Boolean purgeOnStartup);

   /**
    * @deprecated use {@link #asyncStore()} instead
    */
   @Deprecated
   SingletonStoreConfig getSingletonStoreConfig();

   void setSingletonStoreConfig(SingletonStoreConfig singletonStoreConfig);
   
   /**
    * @deprecated use {@link #asyncStore()} instead
    */
   @Deprecated
   AsyncStoreConfig getAsyncStoreConfig();

   void setAsyncStoreConfig(AsyncStoreConfig asyncStoreConfig);
   
   public Boolean isPurgeSynchronously();

   /**
    * @deprecated use {@link #purgeSynchronously(Boolean)} instead
    */
   @Deprecated
   void setPurgeSynchronously(Boolean purgeSynchronously);
   
   CacheStoreConfig purgeSynchronously(Boolean purgeSynchronously);

   Integer getPurgerThreads();

   CacheStoreConfig purgerThreads(Integer purgerThreads);

   AsyncStoreConfig asyncStore();

   SingletonStoreConfig singletonStore();

}
