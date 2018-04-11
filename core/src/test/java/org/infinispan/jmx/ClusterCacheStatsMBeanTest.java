package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;

import java.io.Serializable;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.stats.impl.AbstractClusterStats;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 *
 */
@Test(groups = "functional", testName = "jmx.ClusterCacheStatsMBeanTest")
public class ClusterCacheStatsMBeanTest extends AbstractClusterMBeanTest {

   public ClusterCacheStatsMBeanTest() {
      super(ClusterCacheStatsMBeanTest.class.getName());
   }

   public void testClusterStats() throws Exception {
      Cache<String, Serializable> cache1 = manager(0).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName clusterStats = getCacheObjectName(jmxDomain, cachename + "(repl_sync)", "ClusterCacheStats");

      mBeanServer.setAttribute(clusterStats, new Attribute("StatisticsEnabled", false));
      assert !(boolean) mBeanServer.getAttribute(clusterStats, "StatisticsEnabled");
      mBeanServer.setAttribute(clusterStats, new Attribute("StatisticsEnabled", true));
      assert (boolean) mBeanServer.getAttribute(clusterStats, "StatisticsEnabled");

      long newStaleThreshold = AbstractClusterStats.DEFAULT_STALE_STATS_THRESHOLD - 1;
      mBeanServer.setAttribute(clusterStats, new Attribute("StaleStatsThreshold", newStaleThreshold));
      assertAttributeValue(mBeanServer, clusterStats, "StaleStatsThreshold", newStaleThreshold);

      cache1.put("a1", "b1");
      cache1.put("a2", "b2");
      cache1.put("a3", "b3");
      cache1.put("a4", "b4");

      assertAttributeValue(mBeanServer, clusterStats, "HitRatio", 0.0);
      assertAttributeValue(mBeanServer, clusterStats, "NumberOfEntries", 4);
      assertAttributeValue(mBeanServer, clusterStats, "Stores", 4);
      assertAttributeValue(mBeanServer, clusterStats, "Evictions", 0);

      cache1.remove("a1");
      cache1.get("a1");
      cache1.get("a2");

      //sleep so we pick up refreshed values after remove
      TestingUtil.sleepThread(AbstractClusterStats.DEFAULT_STALE_STATS_THRESHOLD + 1000);

      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "AverageWriteTime", 0);
      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "AverageReadTime", 0);
      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "AverageRemoveTime", 0);

      assertAttributeValue(mBeanServer, clusterStats, "HitRatio", 0.5);
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
}
