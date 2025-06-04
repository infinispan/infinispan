package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "jmx.CacheOpsTest")
public class CacheOpsTest extends SingleCacheManagerTest {

   private static final String JMX_DOMAIN = CacheOpsTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.jmx().enabled(true).domain(JMX_DOMAIN).mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.transaction().autoCommit(false)
         .memory().maxCount(1000);
      return TestCacheManagerFactory.createCacheManager(gcb, cfg);
   }

   public void testClear() throws Exception {
      ObjectName cacheObjectName = getCacheObjectName(JMX_DOMAIN, getDefaultCacheName() + "(local)");
      tm().begin();
      cache().put("k", "v");
      tm().commit();
      assertFalse(cache().isEmpty());
      mBeanServerLookup.getMBeanServer().invoke(cacheObjectName, "clear", new Object[0], new String[0]);
      assertTrue(cache().isEmpty());
   }
}
