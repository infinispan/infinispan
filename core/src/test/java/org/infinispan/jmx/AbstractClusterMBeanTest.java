package org.infinispan.jmx;

import javax.management.MBeanServer;
import javax.management.ObjectName;

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
      GlobalConfigurationBuilder gcb1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb1.globalJmxStatistics().enable().allowDuplicateDomains(true).jmxDomain(jmxDomain)
            .mBeanServerLookup(new PerThreadMBeanServerLookup());
      CacheContainer cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(gcb1, defaultConfig,
            new TransportFlags(), true);
      cacheManager1.start();

      GlobalConfigurationBuilder gcb2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb2.globalJmxStatistics().enable().allowDuplicateDomains(true).jmxDomain(jmxDomain)
            .mBeanServerLookup(new PerThreadMBeanServerLookup());
      CacheContainer cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(gcb2, defaultConfig,
            new TransportFlags(), true);
      cacheManager2.start();

      registerCacheManager(cacheManager1, cacheManager2);

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC).jmxStatistics().enable();
      defineConfigurationOnAllManagers(cachename, cb);
      waitForClusterToForm(cachename);
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
