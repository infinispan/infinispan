package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.testng.AssertJUnit.assertEquals;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.impl.DefaultDataContainer;
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

   private static final String JMX_DOMAIN = CacheConfigurationMBeanTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.jmx().enabled(true).domain(JMX_DOMAIN).mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder dcc = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      dcc.transaction().autoCommit(false);
      dcc.memory().maxCount(1000);
      return TestCacheManagerFactory.createCacheManager(gcb, dcc);
   }

   public void testEvictionSize() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName defaultOn = getCacheObjectName(JMX_DOMAIN, getDefaultCacheName() + "(local)", "Configuration");
      assertEquals(1000L, (long) mBeanServer.getAttribute(defaultOn, "evictionSize"));
      assertEquals(1000, cache().getCacheConfiguration().memory().maxCount());
      DefaultDataContainer<Object, Object> dataContainer = (DefaultDataContainer<Object, Object>) cache()
            .getAdvancedCache().getDataContainer();
      assertEquals(1000, dataContainer.capacity());
      mBeanServer.setAttribute(defaultOn, new Attribute("evictionSize", 2000L));
      assertEquals(2000, cache().getCacheConfiguration().memory().maxCount());
      assertEquals(2000, dataContainer.capacity());
   }
}
