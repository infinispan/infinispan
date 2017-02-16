package org.infinispan.health;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.health.impl.ClusterHealthImpl;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "health.ClusterHealthImplTest", groups = "functional")
public class ClusterHealthImplTest {

    private EmbeddedCacheManager cacheManager;
    private ClusterHealth clusterHealth;

    @BeforeClass
    private void init() {
        GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder().clusteredDefault();
        globalConfigurationBuilder.transport().clusterName("test").nodeName("test");

        cacheManager = new DefaultCacheManager(globalConfigurationBuilder.build());
        cacheManager.defineConfiguration("test", new ConfigurationBuilder().build());
        clusterHealth = new ClusterHealthImpl(cacheManager);
    }

    @AfterClass
    private void cleanUp() {
        if (cacheManager != null) {
            cacheManager.stop();
            cacheManager = null;
        }
    }

    @Test
    public void testReturningClusterName() throws Exception {
        //when
        String clusterName = clusterHealth.getClusterName();

        //then
        assertEquals(clusterName, "test");
    }

    @Test
    public void testReturningHealthyStatus() throws Exception {
        //given
        cacheManager.getCache("test", true);

        //when
        HealthStatus healthStatus = clusterHealth.getHealthStatus();

        //then
        assertEquals(healthStatus, HealthStatus.HEALTHY);
    }

    @Test
    public void testReturningNodeName() throws Exception {
        //when
        String nodeName = clusterHealth.getNodeNames().get(0);

        //then
        assertTrue(nodeName.contains("test"));
    }

    @Test
    public void testReturningNumberOfNodes() throws Exception {
        //when
        int numberOfNodes = clusterHealth.getNumberOfNodes();

        //then
        assertEquals(numberOfNodes, 1);
    }

    @Test
    public void testReturningNumberOfNodesWithNullTransport() throws Exception {
        //given
        DefaultCacheManager cacheManagerWithNullTransport = mock(DefaultCacheManager.class);
        clusterHealth = new ClusterHealthImpl(cacheManagerWithNullTransport);

        //when
        int numberOfNodes = clusterHealth.getNumberOfNodes();

        //then
        assertEquals(numberOfNodes, 1);
    }

    @Test
    public void testReturningNodeNamesWithNullTransport() throws Exception {
        //given
        DefaultCacheManager cacheManagerWithNullTransport = mock(DefaultCacheManager.class);
        clusterHealth = new ClusterHealthImpl(cacheManagerWithNullTransport);

        //when
        List<String> nodeNames = clusterHealth.getNodeNames();

        //then
        assertEquals(nodeNames.size(), 0);
    }

}
