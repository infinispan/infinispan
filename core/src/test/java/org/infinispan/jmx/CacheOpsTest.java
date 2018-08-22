package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "jmx.CacheOpsTest")
public class CacheOpsTest extends SingleCacheManagerTest {

   public static final String JMX_DOMAIN = CacheOpsTest.class.getSimpleName();
   private MBeanServer server;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.globalJmxStatistics().jmxDomain(JMX_DOMAIN).mBeanServerLookup(new PerThreadMBeanServerLookup()).enable();
      ConfigurationBuilder dcc = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      dcc.transaction().autoCommit(false);
      dcc.memory().size(1000);
      dcc.jmxStatistics().enable();
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      return new DefaultCacheManager(gcb.build(), dcc.build());
   }

   public void testClear() throws Exception {
      ObjectName defaultOn = getCacheObjectName(JMX_DOMAIN);
      tm().begin();
      cache().put("k","v");
      tm().commit();
      assertFalse(cache().isEmpty());
      server.invoke(defaultOn, "clear", new Object[]{}, new String[]{});
      assertTrue(cache().isEmpty());
   }
}
