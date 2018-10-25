package org.infinispan.server.test.jmx.management;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.SERVER2_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.getAttribute;
import static org.infinispan.server.test.util.ITestUtils.invokeOperation;
import static org.infinispan.server.test.util.ITestUtils.sleepForSecs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.management.ObjectName;

import org.infinispan.Version;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.infinispan.server.test.category.Unstable;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test that JMX statistics/operations are available for an Infinispan server instance.
 * <p>
 * TODO: operations/attributes of Transactions MBean  - Transactions are only available in embedded mode (to be impl.
 * for HotRod later: ISPN-375)
 * <p>
 * operations/attributes of RecoveryAdmin MBean - the same as above
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "jmx-management-1"),@RunningServer(name = "jmx-management-2")})
public class JmxManagementIT {
    final String JMX_DOMAIN = "jboss." + InfinispanSubsystem.SUBSYSTEM_NAME;
    /* cache MBeans */
    final String distCachePrefix = JMX_DOMAIN + ":type=Cache,name=\"default(dist_sync)\",manager=\"clustered\",component=";
    final String memcachedCachePrefix = JMX_DOMAIN + ":type=Cache,name=\"memcachedCache(dist_sync)\",manager=\"clustered\",component=";
    final String distCacheMBean = distCachePrefix + "Cache";
    final String distributionManagerMBean = distCachePrefix + "DistributionManager";
    // was renamed from DistributedStateTransferManager to StateTransferManager in 6.1.0ER1
    final String distributionStateTransferManagerMBean = distCachePrefix + "StateTransferManager";
    final String lockManagerMBean = distCachePrefix + "LockManager";
    final String rpcManagerMBean = distCachePrefix + "RpcManager";
    final String distCacheStatisticsMBean = distCachePrefix + "Statistics";
    final String memcachedCacheStatisticsMBean = memcachedCachePrefix + "Statistics";
    final String newExtraCacheMBean = JMX_DOMAIN + ":type=Cache,name=\"extracache(local)\",manager=\"clustered\",component=Cache";
    /* cache manager MBeans */
    final String managerPrefix = JMX_DOMAIN + ":type=CacheManager,name=\"clustered\",component=";
    final String cacheManagerMBean = managerPrefix + "CacheManager";
    /* server module MBeans */
    final String hotRodServerMBean = JMX_DOMAIN + ":type=Server,name=HotRod,component=Transport";
    final String memCachedServerMBean = JMX_DOMAIN + ":type=Server,name=Memcached,component=Transport";
    final String protocolMBeanPrefix = "jgroups:type=protocol,cluster=\"default\",protocol=";

    @InfinispanResource("jmx-management-1")
    RemoteInfinispanServer server1;

    @InfinispanResource("jmx-management-2")
    RemoteInfinispanServer server2;

    MBeanServerConnectionProvider provider;
    MBeanServerConnectionProvider provider2;
    RemoteCacheManager manager;
    RemoteCache distCache;
    MemcachedClient mc;

    @Before
    public void setUp() throws Exception {
        if (provider == null) { // initialize just once
            provider = new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);
            provider2 = new MBeanServerConnectionProvider(server2.getHotrodEndpoint().getInetAddress().getHostName(), SERVER2_MGMT_PORT);
            Configuration conf = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName()).port(server1
                    .getHotrodEndpoint().getPort()).build();
            manager = new RemoteCacheManager(conf);
            distCache = manager.getCache();
            mc = new MemcachedClient("UTF-8", server1.getMemcachedEndpoint().getInetAddress()
                    .getHostName(), server1.getMemcachedEndpoint().getPort(), 10000);
        }
        resetCacheStatistics();
        distCache.clear();
    }

    private void resetCacheStatistics() throws Exception {
        invokeOperation(provider, memcachedCacheStatisticsMBean, "resetStatistics", null, null);
    }

    private static int getNumberOfGlobalConnections(MBeanServerConnectionProvider provider, String mbean) throws Exception {
        return Integer.parseInt(getAttribute(provider, mbean, "numberOfGlobalConnections"));
    }

    private static int getNumberOfLocalConnections(MBeanServerConnectionProvider provider, String mbean) throws Exception {
        return Integer.parseInt(getAttribute(provider, mbean, "numberOfLocalConnections"));
    }

    @Test
    @Category(Unstable.class) // ISPN-8291
    public void testHotRodConnectionCount() throws Exception {

        // get number of current local/global connections
        int initialLocal = getNumberOfLocalConnections(provider, hotRodServerMBean);
        int initialGlobal = getNumberOfGlobalConnections(provider, hotRodServerMBean);
        assertEquals("Number of global connections obtained from node1 and node2 is not the same", initialGlobal,
              getNumberOfGlobalConnections(provider2, hotRodServerMBean));
        // create another RCM and use it
        Configuration conf = new ConfigurationBuilder()
              .addServer()
                .host(server1.getHotrodEndpoint().getInetAddress().getHostAddress())
                .port(server1.getHotrodEndpoint().getPort()).build();
        RemoteCacheManager manager2 = new RemoteCacheManager(conf);

        manager2.getCache().put("key", "value");

        // local connections increase by 1, global (in both nodes) by 2 (because we have distributed cache with 2 nodes, both nodes are accessed)
        assertEquals(initialLocal + 1, getNumberOfLocalConnections(provider, hotRodServerMBean));
        assertEquals(initialGlobal + 2, getNumberOfGlobalConnections(provider, hotRodServerMBean));
        assertEquals(initialGlobal + 2, getNumberOfGlobalConnections(provider2, hotRodServerMBean));
    }

    @Test
    @Category(Unstable.class) // ISPN-8291
    public void testMemCachedConnectionCount() throws Exception {
        int initialLocal = getNumberOfLocalConnections(provider, memCachedServerMBean);
        int initialGlobal = getNumberOfGlobalConnections(provider, memCachedServerMBean);
        assertEquals("Number of global connections obtained from node1 and node2 is not the same", initialGlobal,
              getNumberOfGlobalConnections(provider2, memCachedServerMBean));

        MemcachedClient mc2 = new MemcachedClient("UTF-8", server1.getMemcachedEndpoint().getInetAddress()
                .getHostName(), server1.getMemcachedEndpoint().getPort(), 10000);
        mc2.set("key", "value");

        // with the memcached endpoint, the connection is counted only once
        assertEquals(initialLocal + 1, getNumberOfLocalConnections(provider, memCachedServerMBean));
        assertEquals(initialGlobal + 1, getNumberOfGlobalConnections(provider, memCachedServerMBean));
        assertEquals(initialGlobal + 1, getNumberOfGlobalConnections(provider2, memCachedServerMBean));
    }

    @Test
    public void testHotRodServerAttributes() throws Exception {
        distCache.put("key1", new byte[]{1, 2, 3, 4, 5});
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, hotRodServerMBean, "TotalBytesRead")));
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, hotRodServerMBean, "TotalBytesWritten")));
        assertEquals(11222, Integer.parseInt(getAttribute(provider, hotRodServerMBean, "Port")));
        assertEquals(Boolean.TRUE, Boolean.parseBoolean(getAttribute(provider, hotRodServerMBean, "tcpNoDelay")));
        assertEquals(0, Integer.parseInt(getAttribute(provider, hotRodServerMBean, "ReceiveBufferSize")));
        assertEquals(-1, Integer.parseInt(getAttribute(provider, hotRodServerMBean, "IdleTimeout")));
        assertEquals(0, Integer.parseInt(getAttribute(provider, hotRodServerMBean, "SendBufferSize")));
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, hotRodServerMBean, "NumberWorkerThreads")));
        assertNotEquals(0, getAttribute(provider, hotRodServerMBean, "HostName").length());
    }

    @Test
    public void testMemcachedServerAttributes() throws Exception {
        mc.set("key1", "value1");
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, memCachedServerMBean, "TotalBytesRead")));
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, memCachedServerMBean, "TotalBytesWritten")));
        assertEquals(11211, Integer.parseInt(getAttribute(provider, memCachedServerMBean, "Port")));
        assertEquals(Boolean.TRUE, Boolean.parseBoolean(getAttribute(provider, memCachedServerMBean, "tcpNoDelay")));
        assertEquals(0, Integer.parseInt(getAttribute(provider, memCachedServerMBean, "ReceiveBufferSize")));
        assertEquals(-1, Integer.parseInt(getAttribute(provider, memCachedServerMBean, "IdleTimeout")));
        assertEquals(0, Integer.parseInt(getAttribute(provider, memCachedServerMBean, "SendBufferSize")));
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, memCachedServerMBean, "NumberWorkerThreads")));
        assertNotEquals(0, getAttribute(provider, memCachedServerMBean, "HostName").length());
    }

    @Test
    public void testCacheManagerAttributes() throws Exception {
        assertEquals(2, Integer.parseInt(getAttribute(provider, cacheManagerMBean, "CreatedCacheCount")));
        assertEquals(4, Integer.parseInt(getAttribute(provider, cacheManagerMBean, "DefinedCacheCount")));
        assertEquals("clustered", getAttribute(provider, cacheManagerMBean, "Name"));
        assertEquals(2, Integer.parseInt(getAttribute(provider, cacheManagerMBean, "ClusterSize")));
        assertEquals("RUNNING", getAttribute(provider, cacheManagerMBean, "CacheManagerStatus"));
        assertNotEquals(0, getAttribute(provider, cacheManagerMBean, "ClusterMembers").length());
        assertNotEquals(0, getAttribute(provider, cacheManagerMBean, "NodeAddress").length());
        assertEquals(2, Integer.parseInt(getAttribute(provider, cacheManagerMBean, "RunningCacheCount")));
        assertNotEquals(0, getAttribute(provider, cacheManagerMBean, "PhysicalAddresses").length());
        assertEquals(Version.getVersion(), getAttribute(provider, cacheManagerMBean, "Version"));
        String names = getAttribute(provider, cacheManagerMBean, "DefinedCacheNames");
        assertTrue(names.contains("default") && names.contains("memcachedCache"));
    }

    @Test
    public void testDefaultCacheAttributes() throws Exception {
        assertTrue(getAttribute(provider, distCacheMBean, "CacheName").contains("default"));
        assertEquals("RUNNING", getAttribute(provider, distCacheMBean, "CacheStatus"));
    }

    @Test
    public void testDefaultCacheOperations() throws Exception {
        assertEquals("RUNNING", getAttribute(provider, distCacheMBean, "CacheStatus"));
        invokeOperation(provider, distCacheMBean, "stop", null, null);
        assertEquals("TERMINATED", getAttribute(provider, distCacheMBean, "CacheStatus"));
        invokeOperation(provider, distCacheMBean, "start", null, null);
        assertEquals("RUNNING", getAttribute(provider, distCacheMBean, "CacheStatus"));
    }

    @Test
    public void testDistributionStateTransferManagerAttributes() throws Exception {
        assertEquals(Boolean.FALSE, Boolean.parseBoolean(getAttribute(provider, distributionStateTransferManagerMBean, "StateTransferInProgress")));
        assertEquals(Boolean.TRUE, Boolean.parseBoolean(getAttribute(provider, distributionStateTransferManagerMBean, "JoinComplete")));
    }

    @Test
    public void testDistributionManagerOperations() throws Exception {
        distCache.put("key2", "value1");
        assertEquals(Boolean.FALSE, Boolean.parseBoolean(invokeOperation(provider, distributionManagerMBean, "isAffectedByRehash",
                new Object[]{"key2"}, new String[]{"java.lang.Object"}).toString()));
        assertEquals(Boolean.TRUE, Boolean.parseBoolean(invokeOperation(provider, distributionManagerMBean, "isLocatedLocally",
                new Object[]{"key2"}, new String[]{"java.lang.String"}).toString()));
        Object keyLocation = invokeOperation(provider, distributionManagerMBean, "locateKey", new Object[]{"key1"}, new String[]{"java.lang.String"});
        assertTrue(keyLocation instanceof List);
    }

    @Test
    public void testLockManagerAttributes() throws Exception {
        assertEquals(0, Integer.parseInt(getAttribute(provider, lockManagerMBean, "NumberOfLocksHeld")));
        assertEquals(0, Integer.parseInt(getAttribute(provider, lockManagerMBean, "NumberOfLocksAvailable")));
        assertEquals(1000, Integer.parseInt(getAttribute(provider, lockManagerMBean, "ConcurrencyLevel")));
    }

    @Test
    public void testCacheStatisticsAttributes() throws Exception {
        mc.set("key1", "value1");
        mc.set("key2", "value2");
        mc.get("key1");
        assertEquals(2, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "NumberOfEntries")));
        assertEquals(1, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "NumberOfEntriesInMemory")));
        mc.delete("key1");
        assertEquals(2, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "Evictions")));
        assertEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "RemoveMisses")));
        assertNotEquals(0.0, Double.parseDouble(getAttribute(provider, memcachedCacheStatisticsMBean, "ReadWriteRatio")));
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "Hits")));
        assertEquals(Boolean.TRUE, Boolean.parseBoolean(getAttribute(provider, memcachedCacheStatisticsMBean, "StatisticsEnabled")));
        sleepForSecs(2);
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "TimeSinceReset")));
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "ElapsedTime")));
        assertEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "Misses")));
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "RemoveHits")));
        assertNotEquals(null, getAttribute(provider, memcachedCacheStatisticsMBean, "AverageWriteTime"));
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "Stores")));
        assertTrue(1.0 == Double.parseDouble(getAttribute(provider, memcachedCacheStatisticsMBean, "HitRatio")));
        assertNotEquals(null, getAttribute(provider, memcachedCacheStatisticsMBean, "AverageReadTime"));
    }

    @Test
    public void testCacheStatisticsOperations() throws Exception {
        resetCacheStatistics();
        mc.set("key1", "value1");
        assertEquals(1, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "Stores")));
        resetCacheStatistics();
        assertEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "Stores")));
    }

    @Test
    public void testRpcManagerAttributes() throws Exception {
        distCache.put("key1", "value1");
        distCache.put("key2", "value2");
        distCache.put("key3", "value3");
        Integer.parseInt(getAttribute(provider, rpcManagerMBean, "AverageReplicationTime"));
        assertTrue(1.0 == Double.parseDouble(getAttribute(provider, rpcManagerMBean, "SuccessRatioFloatingPoint")));
        assertEquals(0, Integer.parseInt(getAttribute(provider, rpcManagerMBean, "ReplicationFailures")));
        assertEquals(Boolean.TRUE, Boolean.parseBoolean(getAttribute(provider, rpcManagerMBean, "StatisticsEnabled")));
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, rpcManagerMBean, "ReplicationCount")));
        assertEquals("100%", getAttribute(provider, rpcManagerMBean, "SuccessRatio"));
    }

    @Test
    public void testRpcManagerOperations() throws Exception {
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, rpcManagerMBean, "ReplicationCount")));
        invokeOperation(provider, rpcManagerMBean, "resetStatistics", null, null);
        assertEquals(0, Integer.parseInt(getAttribute(provider, rpcManagerMBean, "ReplicationCount")));
    }

    /* for channel and protocol MBeans, test only they're registered, not all the attributes/operations */
    @Test
    public void testJGroupsChannelMBeanAvailable() throws Exception {
        assertTrue(provider.getConnection().isRegistered(new ObjectName("jgroups:type=channel,cluster=\"cluster\"")));
    }
}
