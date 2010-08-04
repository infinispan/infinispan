package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.TestException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredConfig;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;

@Test(groups = "functional", testName = "distribution.UnknownCacheStartTest")
public class UnknownCacheStartTest extends AbstractInfinispanTest {

   Configuration configuration;
   EmbeddedCacheManager cm1, cm2;

   @BeforeTest
   public void setUp() {
      configuration = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
   }

   @AfterTest
   public void tearDown() {
      killCacheManagers(cm1, cm2);
   }

   @Test(timeOut = 60000)
   public void testStartingUnknownCaches() throws Throwable {
      try {
         cm1 = createCacheManager(configuration);

         cm1.defineConfiguration("new_1", configuration);

         Cache<String, String> c1 = cm1.getCache();
         Cache<String, String> c1_new = cm1.getCache("new_1");

         c1.put("k", "v");
         c1_new.put("k", "v");

         assert "v".equals(c1.get("k"));
         assert "v".equals(c1_new.get("k"));

         cm2 = createCacheManager(configuration);
         cm2.defineConfiguration("new_2", configuration);

         Cache<String, String> c2 = cm2.getCache();
         Cache<String, String> c2_new = cm2.getCache("new_AND_DEFINITELY_UNKNOWN_cache_2");

         c2.put("k", "v");
         c2_new.put("k", "v");

         assert "v".equals(c2.get("k"));
         assert "v".equals(c2_new.get("k"));

         BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(c2, c2_new);

         assert false : "Should have thrown an exception!";
      } catch (CacheException expected) {
         // this is good
      }
   }
}
