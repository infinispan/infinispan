package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.testng.AssertJUnit.assertEquals;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 8.1
 */
@Test(groups = "functional", testName = "jmx.CacheConfigurationMBeanTest")
public class CacheConfigurationMBeanTest extends SingleCacheManagerTest {

   public static final String JMX_DOMAIN = CacheConfigurationMBeanTest.class.getSimpleName();
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

   public void testEvictionSize() throws Exception {
      ObjectName defaultOn = getCacheObjectName(JMX_DOMAIN, "___defaultcache(local)", "Configuration");
      assertEquals(1000L, (long) server.getAttribute(defaultOn, "evictionSize"));
      assertEquals(1000, cache().getCacheConfiguration().memory().size());
      DefaultDataContainer<Object, Object> dataContainer = (DefaultDataContainer<Object, Object>) cache()
            .getAdvancedCache().getDataContainer();
      assertEquals(1000, dataContainer.capacity());
      server.setAttribute(defaultOn, new Attribute("evictionSize", 2000L));
      assertEquals(2000, cache().getCacheConfiguration().memory().size());
      assertEquals(2000, dataContainer.capacity());
   }
}
