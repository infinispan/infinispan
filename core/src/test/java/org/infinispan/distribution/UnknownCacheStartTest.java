package org.infinispan.distribution;

import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.TestException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups = "unstable", testName = "distribution.UnknownCacheStartTest", description = "original group: functional")
public class UnknownCacheStartTest extends AbstractInfinispanTest {

   ConfigurationBuilder configuration;
   EmbeddedCacheManager cm1, cm2;

   @BeforeTest
   public void setUp() {
      configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @AfterTest
   public void tearDown() {
      killCacheManagers(cm1, cm2);
   }

   @Test (expectedExceptions = {CacheException.class, TestException.class}, timeOut = 60000)
   public void testStartingUnknownCaches() throws Throwable {
      TestResourceTracker.testThreadStarted(this);

      cm1 = createCacheManager(configuration);

      cm1.defineConfiguration("new_1", configuration.build());

      Cache<String, String> c1 = cm1.getCache();
      Cache<String, String> c1_new = cm1.getCache("new_1");

      c1.put("k", "v");
      c1_new.put("k", "v");

      assertEquals("v", c1.get("k"));
      assertEquals("v", c1_new.get("k"));

      cm2 = createCacheManager(configuration);
      cm2.defineConfiguration("new_2", configuration.build());

      Cache<String, String> c2 = cm2.getCache();
      Cache<String, String> c2_new = cm2.getCache("new_AND_DEFINITELY_UNKNOWN_cache_2");

      c2.put("k", "v");
      c2_new.put("k", "v");

      assertEquals("v", c2.get("k"));
      assertEquals("v", c2_new.get("k"));

      TestingUtil.blockUntilViewsReceived(60000, false, c2, c2_new);
      TestingUtil.waitForStableTopology(c2, c2_new);

      fail("Should have thrown an exception!");
   }
}
