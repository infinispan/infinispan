package org.infinispan.jmx;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
abstract class AbstractClusterMBeanTest extends MultipleCacheManagersTest {

   final String jmxDomain1;
   final String jmxDomain2;
   final String jmxDomain3;

   protected final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   AbstractClusterMBeanTest(String clusterName) {
      this.jmxDomain1 = clusterName;
      this.jmxDomain2 = clusterName + "2";
      this.jmxDomain3 = clusterName + "3";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC).statistics().enable();
      CacheContainer c1 = createManager(cb, jmxDomain1);
      CacheContainer c2 = createManager(cb, jmxDomain2);
      CacheContainer c3 = createManager(cb, jmxDomain3);
      registerCacheManager(c1, c2, c3);
      createCluster(TestDataSCI.INSTANCE, cb, 3);
      waitForClusterToForm(getDefaultCacheName());
   }

   private CacheContainer createManager(ConfigurationBuilder builder, String jmxDomain) {
      GlobalConfigurationBuilder gcb1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb1.cacheContainer().statistics(true)
          .metrics().accurateSize(true)
          .jmx().enabled(true).domain(jmxDomain)
          .mBeanServerLookup(mBeanServerLookup);
      gcb1.serialization().addContextInitializer(TestDataSCI.INSTANCE);
      CacheContainer cacheManager = TestCacheManagerFactory.createClusteredCacheManager(gcb1, builder,
            new TransportFlags());
      cacheManager.start();
      return cacheManager;
   }

   void assertAttributeValue(MBeanServer mBeanServer, ObjectName oName, String attrName, double expectedValue)
         throws Exception {
      String receivedVal = mBeanServer.getAttribute(oName, attrName).toString();
      assert Double.parseDouble(receivedVal) == expectedValue : "expecting " + expectedValue + " for " + attrName
            + ", but received " + receivedVal;
   }

   void assertAttributeValue(MBeanServer mBeanServer, ObjectName oName, String attrName, long expectedValue)
         throws Exception {
      String receivedVal = mBeanServer.getAttribute(oName, attrName).toString();
      assert Long.parseLong(receivedVal) == expectedValue : "expecting " + expectedValue + " for " + attrName
            + ", but received " + receivedVal;
   }

   void assertAttributeValueGreaterThanOrEqualTo(MBeanServer mBeanServer, ObjectName oName, String attrName,
                                                         long valueToCompare) throws Exception {
      String receivedVal = mBeanServer.getAttribute(oName, attrName).toString();
      assert Long.parseLong(receivedVal) >= valueToCompare : "expecting " + receivedVal + " for " + attrName
            + ", to be greater than " + valueToCompare;
   }
}
