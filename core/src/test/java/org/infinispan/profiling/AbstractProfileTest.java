package org.infinispan.profiling;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

@Test(groups = "profiling", enabled = false, testName = "profiling.SingleCacheManagerTest")
public abstract class AbstractProfileTest extends SingleCacheManagerTest {

   protected static final String LOCAL_CACHE_NAME = "local";
   protected static final String REPL_SYNC_CACHE_NAME = "repl_sync";

   protected CacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setConcurrencyLevel(2000);
      cacheManager = new DefaultCacheManager(GlobalConfiguration.getClusteredDefault());
      cacheManager.defineConfiguration(LOCAL_CACHE_NAME, cfg);
      Configuration replCfg = cfg.clone();
      replCfg.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      replCfg.setFetchInMemoryState(false);
      cacheManager.defineConfiguration(REPL_SYNC_CACHE_NAME, replCfg);
      return cacheManager;
   }
}
