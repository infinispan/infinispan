package org.infinispan.jmx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "jmx.CacheClearTest")
public class CacheClearTest extends SingleCacheManagerTest {

   public static final String JMX_DOMAIN = CacheClearTest.class.getSimpleName();
   private MBeanServer server;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.globalJmxStatistics().jmxDomain(JMX_DOMAIN).mBeanServerLookup(new PerThreadMBeanServerLookup()).enable();
      ConfigurationBuilder dcc = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      dcc.transaction().autoCommit(false);
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
