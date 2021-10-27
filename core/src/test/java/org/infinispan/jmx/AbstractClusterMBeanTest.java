package org.infinispan.jmx;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
abstract class AbstractClusterMBeanTest extends MultipleCacheManagersTest {

   final String jmxDomain1;
   final String jmxDomain2;
   final String jmxDomain3;
   final ControlledTimeService timeService = new ControlledTimeService();

   protected final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   AbstractClusterMBeanTest(String clusterName) {
      this.jmxDomain1 = clusterName;
      this.jmxDomain2 = clusterName + "2";
      this.jmxDomain3 = clusterName + "3";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createManager(jmxDomain1);
      createManager(jmxDomain2);
      createManager(jmxDomain3);
      waitForClusterToForm(getDefaultCacheName());
   }

   private void createManager(String jmxDomain) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC).statistics().enable();

      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.cacheContainer().statistics(true)
         .metrics().accurateSize(true)
         .metrics().accurateSize(true)
          .jmx().enabled(true).domain(jmxDomain)
         .mBeanServerLookup(mBeanServerLookup)
         .serialization().addContextInitializer(TestDataSCI.INSTANCE);
      gcb.addModule(TestGlobalConfigurationBuilder.class)
         .testGlobalComponent(TimeService.class.getName(), timeService);

      addClusterEnabledCacheManager(gcb, cb);
   }

   void assertAttributeValue(MBeanServer mBeanServer, ObjectName oName, String attrName, double expectedValue)
         throws Exception {
      String receivedVal = mBeanServer.getAttribute(oName, attrName).toString();
      assertEquals(expectedValue, Double.parseDouble(receivedVal));
   }

   void assertAttributeValue(MBeanServer mBeanServer, ObjectName oName, String attrName, long expectedValue)
         throws Exception {
      String receivedVal = mBeanServer.getAttribute(oName, attrName).toString();
      assertEquals(expectedValue, Long.parseLong(receivedVal));
   }

   void assertAttributeValueGreaterThanOrEqualTo(MBeanServer mBeanServer, ObjectName oName, String attrName,
                                                         long valueToCompare) throws Exception {
      String receivedVal = mBeanServer.getAttribute(oName, attrName).toString();
      assertTrue(valueToCompare <= Long.parseLong(receivedVal));
   }
}
