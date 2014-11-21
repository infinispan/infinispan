package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;

import java.io.Serializable;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.stats.impl.ClusterCacheStatsImpl;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 *
 */
@Test(groups = "functional", testName = "jmx.ClusterCacheStatsMBeanTest")
public class ClusterCacheStatsMBeanTest extends MultipleCacheManagersTest {
   private final String cachename = ClusterCacheStatsMBeanTest.class.getSimpleName();
   public static final String JMX_DOMAIN = ClusterCacheStatsMBeanTest.class.getSimpleName();
   public static final String JMX_DOMAIN2 = JMX_DOMAIN + "2";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = new ConfigurationBuilder();
      GlobalConfigurationBuilder gcb1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb1.globalJmxStatistics().enable().allowDuplicateDomains(true).jmxDomain(JMX_DOMAIN)
            .mBeanServerLookup(new PerThreadMBeanServerLookup());
      CacheContainer cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(gcb1, defaultConfig,
            new TransportFlags(), true);
      cacheManager1.start();

      GlobalConfigurationBuilder gcb2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb2.globalJmxStatistics().enable().allowDuplicateDomains(true).jmxDomain(JMX_DOMAIN)
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

   public void testClusterStats() throws Exception {
      Cache<String, Serializable> cache1 = manager(0).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName clusterStats = getCacheObjectName(JMX_DOMAIN, cachename + "(repl_sync)", "ClusterCacheStats");

      cache1.put("a1", "b1");
      cache1.put("a2", "b2");
      cache1.put("a3", "b3");
      cache1.put("a4", "b4");

      assertAttributeValue(mBeanServer, clusterStats, "NumberOfEntries", 8);
      assertAttributeValue(mBeanServer, clusterStats, "Stores", 4);
      assertAttributeValue(mBeanServer, clusterStats, "Evictions", 0);
      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "AverageWriteTime", 0);

      cache1.remove("a1");

      //sleep so we pick up refreshed values after remove
      TestingUtil.sleepThread(ClusterCacheStatsImpl.DEFAULT_STALE_STATS_THRESHOLD + 1000);

      assertAttributeValue(mBeanServer, clusterStats, "RemoveHits", 1);
      assertAttributeValue(mBeanServer, clusterStats, "RemoveMisses", 0);

      assertAttributeValue(mBeanServer, clusterStats, "NumberOfLocksAvailable", 0);
      assertAttributeValue(mBeanServer, clusterStats, "NumberOfLocksHeld", 0);

      assertAttributeValue(mBeanServer, clusterStats, "Activations", 0);
      assertAttributeValue(mBeanServer, clusterStats, "Passivations", 0);
      assertAttributeValue(mBeanServer, clusterStats, "Invalidations", 0);

      assertAttributeValue(mBeanServer, clusterStats, "CacheLoaderLoads", 0);
      assertAttributeValue(mBeanServer, clusterStats, "CacheLoaderMisses", 0);
      assertAttributeValue(mBeanServer, clusterStats, "StoreWrites", 0);

   }

   private void assertAttributeValue(MBeanServer mBeanServer, ObjectName oName, String attrName, long expectedValue)
         throws Exception {
      String receivedVal = mBeanServer.getAttribute(oName, attrName).toString();
      assert Long.parseLong(receivedVal) == expectedValue : "expecting " + expectedValue + " for " + attrName
            + ", but received " + receivedVal;
   }

   private void assertAttributeValueGreaterThanOrEqualTo(MBeanServer mBeanServer, ObjectName oName, String attrName,
         long valueToCompare) throws Exception {
      String receivedVal = mBeanServer.getAttribute(oName, attrName).toString();
      assert Long.parseLong(receivedVal) >= valueToCompare : "expecting " + receivedVal + " for " + attrName
            + ", to be greater than" + valueToCompare;
   }
}
