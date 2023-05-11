package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Reproducer for ISPN-13191 Deadlock: expiration blocks state transfer
 */
@Test(testName = "statetransfer.StateTransferExpiredStoreTest", groups = "functional")
public class StateTransferExpiredStoreTest extends MultipleCacheManagersTest {
   private ControlledTimeService timeService;

   @Override
   protected void createCacheManagers() throws Throwable {
      timeService = new ControlledTimeService();
      createCluster(TestDataSCI.INSTANCE, null, 2);

      TestingUtil.replaceComponent(manager(0), TimeService.class, timeService, true);
      TestingUtil.replaceComponent(manager(1), TimeService.class, timeService, true);
   }

   private ConfigurationBuilder getConfigurationBuilder(CacheMode cacheMode) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence()
         .addStore(DummyInMemoryStoreConfigurationBuilder.class)
         .fetchPersistentState(true);

      cfg.clustering().cacheMode(cacheMode);
      cfg.clustering().stateTransfer().timeout(30_000);
      return cfg;
   }

   @DataProvider
   Object[][] cacheModes() {
      return new Object[][] {
            {CacheMode.DIST_SYNC},
            {CacheMode.REPL_SYNC},
      };
   }

   @Test(dataProvider = "cacheModes")
   public void testStateTransfer(CacheMode cacheMode) {
      String cacheName = "cache_" + cacheMode;
      Configuration configuration = getConfigurationBuilder(cacheMode).build();
      manager(0).defineConfiguration(cacheName, configuration);
      manager(1).defineConfiguration(cacheName, configuration);

      AdvancedCache<Object, Object> cache0 = advancedCache(0, cacheName);
      String value = "value";
      cache0.put("immortal", value);
      for (int i = 1; i <= 3; i++) {
         cache0.put("lifespan+maxidle" + i, value, i, SECONDS, i, SECONDS);
         cache0.put("lifespan" + i, value, i, SECONDS);
         cache0.put("maxidle" + i, value, -1, SECONDS, i, SECONDS);
         cache0.put("lifespan+maxidle" + i, value, i, SECONDS, i, SECONDS);
      }

      log.info("timeService.advance");
      timeService.advance(SECONDS.toMillis(2));

      AdvancedCache<Object, Object> cache1 = advancedCache(1, cacheName);
      assertEquals(value, cache1.get("immortal"));
      for (int i = 2; i <= 3; i++) {
         assertEquals(value, cache0.get("lifespan+maxidle" + i));
         assertEquals(value, cache0.get("lifespan" + i));
         assertEquals(value, cache0.get("maxidle" + i));
         assertEquals(value, cache0.get("lifespan+maxidle" + i));
      }
   }
}
