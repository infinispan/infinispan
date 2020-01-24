package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "jmx.ClusteredCacheMgmtInterceptorMBeanTest")
public class ClusteredCacheMgmtInterceptorMBeanTest extends MultipleCacheManagersTest {

   private static final String JMX_DOMAIN_1 = ClusteredCacheMgmtInterceptorMBeanTest.class.getSimpleName() + "-1";
   private static final String JMX_DOMAIN_2 = ClusteredCacheMgmtInterceptorMBeanTest.class.getSimpleName() + "-2";
   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalBuilder, JMX_DOMAIN_1, mBeanServerLookup);
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.statistics().enable();

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, builder);

      GlobalConfigurationBuilder globalBuilder2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalBuilder2, JMX_DOMAIN_2, mBeanServerLookup);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(globalBuilder2, builder);

      registerCacheManager(cm1, cm2);
   }

   public void testCorrectStatsInCluster() throws Exception {
      Cache<String, String> cache1 = cache(0);
      Cache<String, String> cache2 = cache(1);
      cache1.put("k", "v");
      assertEquals("v", cache2.get("k"));
      ObjectName stats1 = getCacheObjectName(JMX_DOMAIN_1, getDefaultCacheName() + "(repl_sync)", "Statistics");
      ObjectName stats2 = getCacheObjectName(JMX_DOMAIN_2, getDefaultCacheName() + "(repl_sync)", "Statistics");

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      assertEquals(1L, mBeanServer.getAttribute(stats1, "Stores"));
      assertEquals(0L, mBeanServer.getAttribute(stats2, "Stores"));

      Map<String, String> values = new HashMap<>();
      values.put("k1", "v1");
      values.put("k2", "v2");
      values.put("k3", "v3");
      cache2.putAll(values);

      assertEquals(1L, mBeanServer.getAttribute(stats1, "Stores"));
      assertEquals(3L, mBeanServer.getAttribute(stats2, "Stores"));

      cache1.remove("k");

      assertEquals(1L, mBeanServer.getAttribute(stats1, "RemoveHits"));
      assertEquals(0L, mBeanServer.getAttribute(stats2, "RemoveHits"));
   }
}
