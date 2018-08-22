package org.infinispan.jmx;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
abstract class AbstractClusterMBeanTest extends MultipleCacheManagersTest {

   final String cachename;
   final String jmxDomain;
   final String jmxDomain2;

   AbstractClusterMBeanTest(String clusterName) {
      this.cachename = clusterName;
      this.jmxDomain = clusterName;
      this.jmxDomain2 = clusterName + "2";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = new ConfigurationBuilder();
      CacheContainer c1 = createManager(defaultConfig);
      CacheContainer c2 = createManager(defaultConfig);
      CacheContainer c3 = createManager(defaultConfig);
      registerCacheManager(c1, c2, c3);

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC).jmxStatistics().enable();
      defineConfigurationOnAllManagers(cachename, cb);
      waitForClusterToForm(cachename);
   }

   private CacheContainer createManager(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder gcb1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb1.globalJmxStatistics().enable().jmxDomain(jmxDomain)
            .mBeanServerLookup(new PerThreadMBeanServerLookup());
      CacheContainer cacheManager = TestCacheManagerFactory.createClusteredCacheManager(gcb1, builder,
            new TransportFlags(), true);
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
