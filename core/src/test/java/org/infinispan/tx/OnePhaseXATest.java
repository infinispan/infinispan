package org.infinispan.tx;

import java.util.ArrayList;
import java.util.List;

import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "tx.OnePhaseXATest", description = "See ISPN-156 for details.")
public class OnePhaseXATest extends AbstractInfinispanTest {
   private List<Cache> caches;
   private List<EmbeddedCacheManager> cacheContainers;
   public static final int CACHES_NUM = 2;

   public void testMultipleCaches() throws Exception {

      //add something  to cache
      int i = 0;
      for (Cache c : caches) {
         TransactionManager tm = TestingUtil.getTransactionManager(c);
         tm.begin();
         c.put("key" + i, "value");
         tm.commit();
         i++;
      }

      //check if caches contain these same keys
      i = 0;
      for (Cache c : caches) {
         assert "value".equals(c.get("key0")) : "Failed getting value for key0 on cache " + i;
         assert "value".equals(c.get("key1")) : "Failed getting value for key1 on cache " + i;
         i++;
      }
   }

   @BeforeClass
   public void setUp() throws Exception {
      caches = new ArrayList<>();
      cacheContainers = new ArrayList<>();
      for (int i = 0; i < CACHES_NUM; i++) caches.add(getCache());
   }

   @AfterClass
   public void tearDown() {
      if (caches != null) TestingUtil.killCacheManagers(cacheContainers);
   }

   private Cache getCache() {
      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();

      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.REPL_SYNC)
            .remoteTimeout(30000)
            .transaction().invocationBatching().enable()
            .locking().lockAcquisitionTimeout(60000).useLockStriping(false);
      EmbeddedCacheManager container = TestCacheManagerFactory.createClusteredCacheManager(gc, c);
      cacheContainers.add(container);
      container.defineConfiguration("TestCache", c.build());
      return container.getCache("TestCache");
   }
}
