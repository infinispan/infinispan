package org.infinispan.profiling;

import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static org.infinispan.configuration.cache.CacheMode.*;

@Test(groups = "profiling", testName = "profiling.AbstractProfileTest")
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

   private ConfigurationBuilder getBaseCfg() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.locking().concurrencyLevel(5000).transaction().transactionManagerLookup(new JBossStandaloneJTAManagerLookup());
      return cfg;
   }

   private ConfigurationBuilder getClusteredCfg(CacheMode mode, boolean l1) {
      ConfigurationBuilder cfg = getBaseCfg();
      cfg
         .locking().lockAcquisitionTimeout(60000)
         .clustering().cacheMode(mode).sync().replTimeout(60000).stateTransfer().fetchInMemoryState(false);
      if (mode.isDistributed()) {
         cfg.clustering().l1().enabled(l1).lifespan(120000);
      }
      return cfg;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.asyncTransportExecutor().factory(new WTE());
      cacheManager = TestCacheManagerFactory.createClusteredCacheManager(builder, new ConfigurationBuilder());

      cacheManager.defineConfiguration(LOCAL_CACHE_NAME, getBaseCfg().build());

      cacheManager.defineConfiguration(REPL_SYNC_CACHE_NAME, getClusteredCfg(REPL_SYNC, false).build());
      cacheManager.defineConfiguration(REPL_ASYNC_CACHE_NAME, getClusteredCfg(REPL_ASYNC, false).build());
      cacheManager.defineConfiguration(DIST_SYNC_CACHE_NAME, getClusteredCfg(DIST_SYNC, false).build());
      cacheManager.defineConfiguration(DIST_ASYNC_CACHE_NAME, getClusteredCfg(DIST_ASYNC, false).build());
      cacheManager.defineConfiguration(DIST_SYNC_L1_CACHE_NAME, getClusteredCfg(DIST_SYNC, true).build());
      cacheManager.defineConfiguration(DIST_ASYNC_L1_CACHE_NAME, getClusteredCfg(DIST_ASYNC, true).build());

      return cacheManager;
   }

   public static class WTE implements ExecutorFactory {
      @Override
      public ExecutorService getExecutor(Properties p) {
         return new WithinThreadExecutor();
      }
   }
}
