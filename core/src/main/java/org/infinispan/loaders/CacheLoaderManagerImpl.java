package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import static org.infinispan.context.Flag.SKIP_CACHE_STATUS_CHECK;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.loaders.decorators.AsyncStore;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.decorators.ReadOnlyStore;
import org.infinispan.loaders.decorators.SingletonStore;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CacheLoaderManagerImpl implements CacheLoaderManager {

   Configuration configuration;
   CacheLoaderManagerConfig clmConfig;
   Cache<Object, Object> cache;
   Marshaller m;
   CacheLoader loader;
   private static final Log log = LogFactory.getLog(CacheLoaderManagerImpl.class);

   @Inject
   public void inject(Cache cache, Marshaller marshaller, Configuration configuration) {
      this.cache = cache;
      this.m = marshaller;
      this.configuration = configuration;
   }

   public CacheLoader getCacheLoader() {
      return loader;
   }

   public final CacheStore getCacheStore() {
      if (loader != null && loader instanceof CacheStore) {
         return (CacheStore) loader;
      } else {
         return null;
      }
   }

   public void purge() {
      CacheStore cs = getCacheStore();
      if (cs != null) try {
         cs.clear();
      } catch (CacheLoaderException e) {
         throw new CacheException("Unable to purge cache store", e);
      }
   }

   private void purgeLoaders(boolean force) throws Exception {
      CacheStore cs = getCacheStore();
      if (cs != null) {
         if ((cs instanceof ChainingCacheStore) && !force) {
            ((ChainingCacheStore) loader).purgeIfNecessary();
         } else {
            CacheStoreConfig first = (CacheStoreConfig) clmConfig.getFirstCacheLoaderConfig();
            if (force || (first != null && first.isPurgeOnStartup())) {
               cs.clear();
            }
         }
      }
   }

   public boolean isUsingPassivation() {
      return isEnabled() ? clmConfig.isPassivation() : false;
   }

   public boolean isShared() {
      return isEnabled() ? clmConfig.isShared() : false;
   }

   public boolean isFetchPersistentState() {
      return isEnabled() ? clmConfig.isFetchPersistentState() : false;
   }

   @Start(priority = 10)
   public void start() {
      clmConfig = configuration.getCacheLoaderManagerConfig();
      if (clmConfig != null) {
         try {
            loader = createCacheLoader();
            if (loader != null) loader.start();
            purgeLoaders(false);
         } catch (Exception e) {
            throw new CacheException("Unable to start cache loaders", e);
         }
      }
   }

   public boolean isEnabled() {
      return clmConfig != null;
   }

   /**
    * Performs a preload on the cache based on the cache loader preload configs used when configuring the cache.
    */
   @Start(priority = 50)
   public void preload() {
      if (loader != null) {
         if (clmConfig.isPreload()) {
            log.debug("Preloading transient state from cache loader {0}", loader);
            long start = 0, stop = 0, total = 0;
            if (log.isDebugEnabled()) start = System.currentTimeMillis();
            Set<InternalCacheEntry> state;
            try {
               state = loader.loadAll();
            } catch (CacheLoaderException e) {
               throw new CacheException("Unable to preload!", e);
            }

            for (InternalCacheEntry e : state)
               cache.getAdvancedCache().withFlags(SKIP_CACHE_STATUS_CHECK).put(e.getKey(), e.getValue(), e.getLifespan(), MILLISECONDS, e.getMaxIdle(), MILLISECONDS);

            if (log.isDebugEnabled()) stop = System.currentTimeMillis();
            if (log.isDebugEnabled()) total = stop - start;
            log.debug("Preloaded {0} keys in {1} milliseconds", state.size(), total);
         }
      }
   }

   @Stop
   public void stop() {
      if (loader != null) try {
         loader.stop();
      } catch (CacheLoaderException e) {
         throw new CacheException(e);
      }
      loader = null;
   }

   CacheLoader createCacheLoader() throws Exception {
      CacheLoader tmpLoader;
      // if we only have a single cache loader configured in the chaining cacheloader then
      // don't use a chaining cache loader at all.
      // also if we are using passivation then just directly use the first cache loader.
      if (clmConfig.useChainingCacheLoader()) {
         // create chaining cache loader.
         ChainingCacheStore ccl = new ChainingCacheStore();
         tmpLoader = ccl;

         // only one cache loader may have fetchPersistentState to true.
         int numLoadersWithFetchPersistentState = 0;
         for (CacheLoaderConfig cfg : clmConfig.getCacheLoaderConfigs()) {
            if (cfg instanceof CacheStoreConfig) {
               if (((CacheStoreConfig) cfg).isFetchPersistentState()) numLoadersWithFetchPersistentState++;
               if (numLoadersWithFetchPersistentState > 1)
                  throw new Exception("Invalid cache loader configuration!!  Only ONE cache loader may have fetchPersistentState set to true.  Cache will not start!");
               assertNotSingletonAndShared(((CacheStoreConfig) cfg));
            }

            CacheLoader l = createCacheLoader(cfg, cache);
            ccl.addCacheLoader(l, cfg);
         }
      } else {
         CacheLoaderConfig cfg = clmConfig.getFirstCacheLoaderConfig();
         if (cfg != null) {
            tmpLoader = createCacheLoader(cfg, cache);
            if (cfg instanceof CacheStoreConfig)
            assertNotSingletonAndShared(((CacheStoreConfig) cfg));
         } else {
            return null;
         }
      }

      // Update the config with those actually used by the loaders
      ReflectionUtil.setValue(clmConfig, "accessible", true);

      return tmpLoader;
   }

   CacheLoader createCacheLoader(CacheLoaderConfig cfg, Cache cache) throws Exception {
      CacheLoader tmpLoader = (CacheLoader) Util.getInstance(cfg.getCacheLoaderClassName());

      if (tmpLoader != null) {
         if (cfg instanceof CacheStoreConfig) {
            CacheStore tmpStore = (CacheStore) tmpLoader;
            // async?
            CacheStoreConfig cfg2 = (CacheStoreConfig) cfg;
            if (cfg2.getAsyncStoreConfig().isEnabled()) {
               tmpStore = new AsyncStore(tmpStore, cfg2.getAsyncStoreConfig());
               tmpLoader = tmpStore;
            }

            // read only?
            if (cfg2.isIgnoreModifications()) {
               tmpStore = new ReadOnlyStore(tmpStore);
               tmpLoader = tmpStore;
            }

            // singleton?
            SingletonStoreConfig ssc = cfg2.getSingletonStoreConfig();
            if (ssc != null && ssc.isSingletonStoreEnabled()) {
               tmpStore = new SingletonStore(tmpStore, cache, ssc);
               tmpLoader = tmpStore;
            }
         }

         // load props
         tmpLoader.init(cfg, cache, m);
      }
      return tmpLoader;
   }

   void assertNotSingletonAndShared(CacheStoreConfig cfg) {
      SingletonStoreConfig ssc = cfg.getSingletonStoreConfig();
      if (ssc != null && ssc.isSingletonStoreEnabled() && clmConfig.isShared())
         throw new ConfigurationException("Invalid cache loader configuration!!  If a cache loader is configured as a singleton, the cache loader cannot be shared in a cluster!");
   }
}
