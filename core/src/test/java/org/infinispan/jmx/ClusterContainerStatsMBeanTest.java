package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;

import java.util.stream.Stream;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jmx.ClusterContainerStatsMBeanTest")
public class ClusterContainerStatsMBeanTest extends AbstractClusterMBeanTest {

   private final String componentName;

   public ClusterContainerStatsMBeanTest(String componentName) {
      super(ClusterContainerStatsMBeanTest.class.getName());
      this.componentName = componentName;
   }

   @Override
   public Object[] factory() {
      return Stream.of("ClusterContainerStats", "LocalContainerStats")
            .map(ClusterContainerStatsMBeanTest::new)
            .toArray();
   }

   @Override
   protected String parameters() {
      return String.format("[%s]", componentName);
   }

   public void testContainerStats() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName clusterStats = getCacheManagerObjectName(jmxDomain1, "DefaultCacheManager", componentName);

      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "MemoryAvailable", 1);
      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "MemoryMax", 1);
      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "MemoryTotal", 1);
      assertAttributeValueGreaterThanOrEqualTo(mBeanServer, clusterStats, "MemoryUsed", 1);
   }
}
