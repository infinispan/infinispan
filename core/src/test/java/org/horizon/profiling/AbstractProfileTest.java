package org.horizon.profiling;

import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

@Test(groups = "profiling", enabled = false, testName = "profiling.SingleCacheManagerTest")
public abstract class AbstractProfileTest extends SingleCacheManagerTest {

   protected static final String LOCAL_CACHE_NAME = "local";
   protected static final String REPL_SYNC_CACHE_NAME = "repl_sync";

   protected CacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setConcurrencyLevel(2000);
      cacheManager = new DefaultCacheManager(GlobalConfiguration.getClusteredDefault());
      cacheManager.defineCache(LOCAL_CACHE_NAME, cfg);
      Configuration replCfg = cfg.clone();
      replCfg.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      replCfg.setFetchInMemoryState(false);
      cacheManager.defineCache(REPL_SYNC_CACHE_NAME, replCfg);
      return cacheManager;
   }
}
