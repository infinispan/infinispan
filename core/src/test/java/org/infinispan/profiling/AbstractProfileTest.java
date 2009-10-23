package org.infinispan.profiling;

import org.infinispan.config.Configuration;
import static org.infinispan.config.Configuration.CacheMode.*;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

@Test(groups = "profiling", enabled = false, testName = "profiling.AbstractProfileTest")
public abstract class AbstractProfileTest extends SingleCacheManagerTest {

   protected static final String LOCAL_CACHE_NAME = "local";
   protected static final String REPL_SYNC_CACHE_NAME = "repl_sync";
   protected static final String REPL_ASYNC_CACHE_NAME = "repl_async";
   protected static final String DIST_SYNC_L1_CACHE_NAME = "dist_sync_l1";
   protected static final String DIST_ASYNC_L1_CACHE_NAME = "dist_async_l1";
   protected static final String DIST_SYNC_CACHE_NAME = "dist_sync";
   protected static final String DIST_ASYNC_CACHE_NAME = "dist_async";

   boolean startedInCmdLine = false;
   String clusterNameOverride = null;

   protected void initTest() throws Exception {
      System.out.println("Setting up test params!");
      if (startedInCmdLine) cacheManager = createCacheManager();
   }

   private Configuration getBaseCfg() {
      Configuration cfg = new Configuration();
      cfg.setConcurrencyLevel(5000);
      return cfg;
   }

   private Configuration getClusteredCfg(Configuration.CacheMode mode, boolean l1) {
      Configuration cfg = getBaseCfg();
      cfg.setLockAcquisitionTimeout(60000);
      cfg.setSyncReplTimeout(60000);
      cfg.setCacheMode(mode);
      cfg.setFetchInMemoryState(false);
      if (mode.isDistributed()) {
         cfg.setL1CacheEnabled(l1);
         cfg.setL1Lifespan(120000);
      }
      return cfg;
   }

   protected CacheManager createCacheManager() throws Exception {
      GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
      gc.setAsyncTransportExecutorFactoryClass(WTE.class.getName());
      cacheManager = new DefaultCacheManager(gc);

      cacheManager.defineConfiguration(LOCAL_CACHE_NAME, getBaseCfg());

      cacheManager.defineConfiguration(REPL_SYNC_CACHE_NAME, getClusteredCfg(REPL_SYNC, false));
      cacheManager.defineConfiguration(REPL_ASYNC_CACHE_NAME, getClusteredCfg(REPL_ASYNC, false));
      cacheManager.defineConfiguration(DIST_SYNC_CACHE_NAME, getClusteredCfg(DIST_SYNC, false));
      cacheManager.defineConfiguration(DIST_ASYNC_CACHE_NAME, getClusteredCfg(DIST_ASYNC, false));
      cacheManager.defineConfiguration(DIST_SYNC_L1_CACHE_NAME, getClusteredCfg(DIST_SYNC, true));
      cacheManager.defineConfiguration(DIST_ASYNC_L1_CACHE_NAME, getClusteredCfg(DIST_ASYNC, true));

      return cacheManager;
   }

   public static class WTE implements ExecutorFactory {
      public ExecutorService getExecutor(Properties p) {
         return new WithinThreadExecutor();
      }
   }
}
