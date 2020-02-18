package org.infinispan.jmx;

import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.health.HealthStatus;
import org.infinispan.health.jmx.HealthJMXExposer;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.commons.test.TestResourceTracker;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jmx.HealthJmxTest")
public class HealthJmxTest extends MultipleCacheManagersTest {

    private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

    @Override
    protected void createCacheManagers() throws Throwable {
        addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r1"), getConfigurationBuilder())
              .defineConfiguration("test", getConfigurationBuilder().build());
    }

    private ConfigurationBuilder getConfigurationBuilder() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.clustering().cacheMode(CacheMode.DIST_SYNC)
                .stateTransfer().awaitInitialTransfer(false)
                .partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);
        return cb;
    }

    private GlobalConfigurationBuilder getGlobalConfigurationBuilder(String rackId) {
        GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
        configureJmx(gcb, getClass().getSimpleName(), mBeanServerLookup);
        gcb.transport().rackId(rackId);
        return gcb;
    }

    public void testHealthCheckAPI() throws Exception {
        //given
        //we need this to start a cache with a custom name
        cacheManagers.get(0).getCache("test").put("1", "1");

        MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();

        //when
        String domain0 = manager(0).getCacheManagerConfiguration().jmx().domain();
        ObjectName healthAPI0 = TestingUtil.getCacheManagerObjectName(domain0, "DefaultCacheManager", HealthJMXExposer.OBJECT_NAME);

        Object numberOfCpus = mBeanServer.getAttribute(healthAPI0, "NumberOfCpus");
        Object totalMemoryKb = mBeanServer.getAttribute(healthAPI0, "TotalMemoryKb");
        Object freeMemoryKb = mBeanServer.getAttribute(healthAPI0, "FreeMemoryKb");
        Object clusterHealth = mBeanServer.getAttribute(healthAPI0, "ClusterHealth");
        Object clusterName = mBeanServer.getAttribute(healthAPI0, "ClusterName");
        Object numberOfNodes = mBeanServer.getAttribute(healthAPI0, "NumberOfNodes");
        Object cacheHealth = mBeanServer.getAttribute(healthAPI0, "CacheHealth");

        //then
        assertTrue((int) numberOfCpus > 0);
        assertTrue((long) totalMemoryKb > 0);
        assertTrue((long) freeMemoryKb > 0);
        assertEquals((String) clusterHealth, HealthStatus.HEALTHY.toString());
        assertEquals((String) clusterName, TestResourceTracker.getCurrentTestName());
        assertEquals((int) numberOfNodes, 1);
        assertEquals(((String[]) cacheHealth)[0], "test");
        assertEquals(((String[]) cacheHealth)[1], HealthStatus.HEALTHY.toString());
    }
}
