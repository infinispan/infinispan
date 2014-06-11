package org.infinispan.server.test.jmx.management;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.SERVER2_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.getAttribute;
import static org.infinispan.server.test.util.ITestUtils.invokeOperation;
import static org.infinispan.server.test.util.ITestUtils.sleepForSecs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test that JMX statistics/operations are available for an Infinispan server instance.
 * <p/>
 * TODO: operations/attributes of Transactions MBean  - Transactions are only available in embedded mode (to be impl.
 * for HotRod later: ISPN-375)
 * <p/>
 * operations/attributes of RecoveryAdmin MBean - the same as above
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "jmx-management-1"),@RunningServer(name = "jmx-management-2")})
public class JmxManagementIT {

    /* cache MBeans */
    final String distCachePrefix = "jboss.infinispan:type=Cache,name=\"default(dist_sync)\",manager=\"clustered\",component=";
    final String memcachedCachePrefix = "jboss.infinispan:type=Cache,name=\"memcachedCache(dist_sync)\",manager=\"clustered\",component=";
    final String distCacheMBean = distCachePrefix + "Cache";
    final String distributionManagerMBean = distCachePrefix + "DistributionManager";
    // was renamed from DistributedStateTransferManager to StateTransferManager in 6.1.0ER1
    final String distributionStateTransferManagerMBean = distCachePrefix + "StateTransferManager";
    final String lockManagerMBean = distCachePrefix + "LockManager";
    final String rpcManagerMBean = distCachePrefix + "RpcManager";
    final String distCacheStatisticsMBean = distCachePrefix + "Statistics";
    final String memcachedCacheStatisticsMBean = memcachedCachePrefix + "Statistics";
    final String newExtraCacheMBean = "jboss.infinispan:type=Cache,name=\"extracache(local)\",manager=\"clustered\",component=Cache";
    /* cache manager MBeans */
    final String managerPrefix = "jboss.infinispan:type=CacheManager,name=\"clustered\",component=";
    final String cacheManagerMBean = managerPrefix + "CacheManager";
    /* server module MBeans */
    final String hotRodServerMBean = "jboss.infinispan:type=Server,name=HotRod,component=Transport";
    final String memCachedServerMBean = "jboss.infinispan:type=Server,name=Memcached,component=Transport";
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
    public void setUp() throws IOException {
        provider = new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);
        provider2 = new MBeanServerConnectionProvider(server2.getHotrodEndpoint().getInetAddress().getHostName(), SERVER2_MGMT_PORT);
        if (manager == null) {
            Configuration conf = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName()).port(server1
                    .getHotrodEndpoint().getPort()).build();
            manager = new RemoteCacheManager(conf);
        }
        distCache = manager.getCache();
        mc = new MemcachedClient("UTF-8", server1.getMemcachedEndpoint().getInetAddress()
                .getHostName(), server1.getMemcachedEndpoint().getPort(), 10000);
        assertDefaultCacheEmpty();
    }

    private void assertDefaultCacheEmpty() {
        while (server1.getCacheManager("clustered").getDefaultCache().getNumberOfEntries() != 0 ||
                server2.getCacheManager("clustered").getDefaultCache().getNumberOfEntries() != 0) {
            try {
                distCache.clear();
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testHotRodConnectionCount() throws Exception {

        // get number of current local/global connections
        int initialLocal = Integer.parseInt(getAttribute(provider, hotRodServerMBean, "numberOfLocalConnections"));
        int initialGlobal = Integer.parseInt(getAttribute(provider, hotRodServerMBean, "numberOfGlobalConnections"));
        assertEquals("Number of global connections obtained from node1 and node2 is not the same", initialGlobal,
                Integer.parseInt(getAttribute(provider2, hotRodServerMBean, "numberOfGlobalConnections")));
        // create another RCM and use it
        Configuration conf = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName()).port(server1
                .getHotrodEndpoint().getPort()).build();
        RemoteCacheManager manager2 = new RemoteCacheManager(conf);
        manager2.getCache().put("key", "value");

        // local connections increase by 1, global (in both nodes) by 2 (because we have distributed cache with 2 nodes, both nodes are accessed)
        assertEquals(initialLocal + 1, Integer.parseInt(getAttribute(provider, hotRodServerMBean, "numberOfLocalConnections")));
        assertEquals(initialGlobal + 2, Integer.parseInt(getAttribute(provider, hotRodServerMBean, "numberOfGlobalConnections")));
        assertEquals(initialGlobal + 2, Integer.parseInt(getAttribute(provider2, hotRodServerMBean, "numberOfGlobalConnections")));
    }

    @Test
    public void testMemCachedConnectionCount() throws Exception {
        int initialLocal = Integer.parseInt(getAttribute(provider, memCachedServerMBean, "numberOfLocalConnections"));
        int initialGlobal = Integer.parseInt(getAttribute(provider, memCachedServerMBean, "numberOfGlobalConnections"));
        assertEquals("Number of global connections obtained from node1 and node2 is not the same", initialGlobal,
                Integer.parseInt(getAttribute(provider2, memCachedServerMBean, "numberOfGlobalConnections")));

        MemcachedClient mc2 = new MemcachedClient("UTF-8", server1.getMemcachedEndpoint().getInetAddress()
                .getHostName(), server1.getMemcachedEndpoint().getPort(), 10000);
        mc2.set("key", "value");

        // with the memcached endpoint, the connection is counted only once
        assertEquals(initialLocal + 1, Integer.parseInt(getAttribute(provider, memCachedServerMBean, "numberOfLocalConnections")));
        assertEquals(initialGlobal + 1, Integer.parseInt(getAttribute(provider, memCachedServerMBean, "numberOfGlobalConnections")));
        assertEquals(initialGlobal + 1, Integer.parseInt(getAttribute(provider2, memCachedServerMBean, "numberOfGlobalConnections")));
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
        assertEquals(4, Integer.parseInt(getAttribute(provider, cacheManagerMBean, "CreatedCacheCount")));
        assertEquals(4, Integer.parseInt(getAttribute(provider, cacheManagerMBean, "DefinedCacheCount")));
        assertEquals("clustered", getAttribute(provider, cacheManagerMBean, "Name"));
        assertEquals(2, Integer.parseInt(getAttribute(provider, cacheManagerMBean, "ClusterSize")));
        assertEquals("RUNNING", getAttribute(provider, cacheManagerMBean, "CacheManagerStatus"));
        assertNotEquals(0, getAttribute(provider, cacheManagerMBean, "ClusterMembers").length());
        assertNotEquals(0, getAttribute(provider, cacheManagerMBean, "NodeAddress").length());
        assertEquals(4, Integer.parseInt(getAttribute(provider, cacheManagerMBean, "RunningCacheCount")));
        assertNotEquals(0, getAttribute(provider, cacheManagerMBean, "PhysicalAddresses").length());
        assertEquals(Version.VERSION, getAttribute(provider, cacheManagerMBean, "Version"));
        String names = getAttribute(provider, cacheManagerMBean, "DefinedCacheNames");
        assertTrue(names.contains("default") && names.contains("memcachedCache") &&
                names.contains("hotRodTopologyCache"));
    }

    @Ignore("Not supported - https://bugzilla.redhat.com/show_bug.cgi?id=785105")
    @Test
    public void testCacheManagerOperations() throws Exception {
        assertTrue(!provider.getConnection().isRegistered(new ObjectName(newExtraCacheMBean)));
        invokeOperation(provider, cacheManagerMBean, "startCache", new Object[]{"extracache"}, new String[]{"java.lang.String"});
        assertTrue(provider.getConnection().isRegistered(new ObjectName(newExtraCacheMBean)));
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
        mc.get("key1");
        assertNotEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "NumberOfEntries")));
        mc.delete("key1");
        assertEquals(0, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "Evictions")));
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
        invokeOperation(provider, memcachedCacheStatisticsMBean, "resetStatistics", null, null);
        mc.set("key1", "value1");
        assertEquals(1, Integer.parseInt(getAttribute(provider, memcachedCacheStatisticsMBean, "Stores")));
        invokeOperation(provider, memcachedCacheStatisticsMBean, "resetStatistics", null, null);
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
        assertTrue(provider.getConnection().isRegistered(new ObjectName("jgroups:type=channel,cluster=\"clustered\"")));
    }
}
