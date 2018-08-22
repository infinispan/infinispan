package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jmx.ClusterContainerStatsMBeanTest")
public class ClusterContainerStatsMBeanTest extends AbstractClusterMBeanTest {

   public ClusterContainerStatsMBeanTest() {
      super(ClusterContainerStatsMBeanTest.class.getName());
   }

   public void testContainerStats() throws Exception {
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName clusterStats = getCacheManagerObjectName(jmxDomain, "DefaultCacheManager", "ClusterContainerStats");

      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "MemoryAvailable", 1);
      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "MemoryMax", 1);
      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "MemoryTotal", 1);
      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "MemoryUsed", 1);
   }
}
