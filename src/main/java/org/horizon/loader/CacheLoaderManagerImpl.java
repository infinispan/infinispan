package org.horizon.loader;

import org.horizon.Cache;
import org.horizon.CacheException;
import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.config.Configuration;
import org.horizon.config.ConfigurationException;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.factories.annotations.Stop;
import org.horizon.invocation.Options;
import org.horizon.loader.decorators.AsyncStore;
import org.horizon.loader.decorators.ChainingCacheStore;
import org.horizon.loader.decorators.ReadOnlyStore;
import org.horizon.loader.decorators.SingletonStore;
import org.horizon.loader.decorators.SingletonStoreConfig;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.marshall.Marshaller;
import org.horizon.util.ReflectionUtil;
import org.horizon.util.Util;

import java.util.Set;
import java.util.concurrent.TimeUnit;

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
            CacheLoaderConfig first = clmConfig.getFirstCacheLoaderConfig();
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
            Set<StoredEntry> state;
            try {
               state = loader.loadAll();
            } catch (CacheLoaderException e) {
               throw new CacheException("Unable to preload!", e);
            }

            for (StoredEntry se : state)
               cache.getAdvancedCache().put(se.getKey(), se.getValue(), se.getLifespan(),
                                            TimeUnit.MILLISECONDS, Options.SKIP_CACHE_STATUS_CHECK);

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
            if (cfg.isFetchPersistentState()) numLoadersWithFetchPersistentState++;

            if (numLoadersWithFetchPersistentState > 1)
               throw new Exception("Invalid cache loader configuration!!  Only ONE cache loader may have fetchPersistentState set to true.  Cache will not start!");

            assertNotSingletonAndShared(cfg);

            CacheLoader l = createCacheLoader(cfg, cache);
            ccl.addCacheLoader(l, cfg);
         }
      } else {
         CacheLoaderConfig cfg = clmConfig.getFirstCacheLoaderConfig();
         tmpLoader = createCacheLoader(cfg, cache);
         assertNotSingletonAndShared(cfg);
      }

      // Update the config with those actually used by the loaders
      ReflectionUtil.setValue(clmConfig, "accessible", true);

      return tmpLoader;
   }

   CacheLoader createCacheLoader(CacheLoaderConfig cfg, Cache cache) throws Exception {
      CacheLoader tmpLoader = (CacheLoader) Util.getInstance(cfg.getCacheLoaderClassName());

      if (tmpLoader != null) {
         if (tmpLoader instanceof CacheStore) {
            CacheStore tmpStore = (CacheStore) tmpLoader;
            // async?
            if (cfg.getAsyncStoreConfig().isEnabled()) {
               tmpStore = new AsyncStore(tmpStore, cfg.getAsyncStoreConfig());
               tmpLoader = tmpStore;
            }

            // read only?
            if (cfg.isIgnoreModifications()) {
               tmpStore = new ReadOnlyStore(tmpStore);
               tmpLoader = tmpStore;
            }

            // singleton?
            SingletonStoreConfig ssc = cfg.getSingletonStoreConfig();
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

   void assertNotSingletonAndShared(CacheLoaderConfig cfg) {
      SingletonStoreConfig ssc = cfg.getSingletonStoreConfig();
      if (ssc != null && ssc.isSingletonStoreEnabled() && clmConfig.isShared())
         throw new ConfigurationException("Invalid cache loader configuration!!  If a cache loader is configured as a singleton, the cache loader cannot be shared in a cluster!");
   }
}
