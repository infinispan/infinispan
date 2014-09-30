package org.infinispan.server.test.jmx.suppress.statetransfer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.SERVER2_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.SERVER3_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.infinispan.server.test.util.ITestUtils.getAttribute;
import static org.infinispan.server.test.util.ITestUtils.setAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstact class for testing state transfer suppress functionality
 *
 * @author <a href="mailto:amanukya@redhat.com">Anna Manukyan</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 */
@RunWith(Arquillian.class)
public class StateTransferSuppressIT {

    private static final Logger log = Logger.getLogger(StateTransferSuppressIT.class);

    /* container names */
    protected static final String CONTAINER1 = "suppress-state-transfer-1";
    protected static final String CONTAINER2 = "suppress-state-transfer-2";
    protected static final String CONTAINER3 = "suppress-state-transfer-3";
    private static final int NUMBER_ENTRIES = 1000;

    private static final String CACHE_MANAGER_NAME = "clustered";
    private static final String MEMCACHED_CACHE_NAME = "memcachedCache";
    private static final String HOTROD_CACHE_NAME = "default";

    /* cache MBeans */
    final String HOTROD_DIST_CACHE_PREFIX = "jboss.infinispan:type=Cache,name=\"" + HOTROD_CACHE_NAME + "(dist_sync)\",manager=\"" + getCacheManagerName() + "\",component=";
    final String MEMCACHED_DIST_CACHE_PREFIX = "jboss.infinispan:type=Cache,name=\"" + MEMCACHED_CACHE_NAME + "(dist_sync)\",manager=\"" + getCacheManagerName() + "\",component=";
    final String HOTROD_RPC_MANAGER_MBEAN = HOTROD_DIST_CACHE_PREFIX + "RpcManager";
    final String MEMCACHED_RPC_MANAGER_MBEAN = MEMCACHED_DIST_CACHE_PREFIX + "RpcManager";

    /* JMX attribute names */
    final String REBALANCE_ENABLED_ATTR_NAME = "RebalancingEnabled";
    final String COMMITTED_VIEW_AS_STRING_ATTR_NAME = "CommittedViewAsString";
    final String PENDING_VIEW_AS_STRING_ATTR_NAME = "PendingViewAsString";

    /* JMX result views */
    private final String OWNERS_2_MEMBERS_NODE1_NODE2 = "[node0/" + getCacheManagerName() + ", node1/" + getCacheManagerName() + "]";
    private final String OWNERS_2_MEMBERS_NODE2_NODE3 = "[node1/" + getCacheManagerName() + ", node2/" + getCacheManagerName() + "]";
    private final String OWNERS_2_MEMBERS_NODE1_NODE2_NODE3 = "[node0/" + getCacheManagerName() + ", node1/" + getCacheManagerName() + ", node2/" + getCacheManagerName() + "]";

    /* server module MBeans */
    private final String LOCAL_TOPOLOGY_MANAGER = "jboss.infinispan:type=CacheManager,name=\"" + getCacheManagerName() + "\",component=LocalTopologyManager";

    @InfinispanResource(CONTAINER1)
    RemoteInfinispanServer server1;
    @InfinispanResource(CONTAINER2)
    RemoteInfinispanServer server2;
    @InfinispanResource(CONTAINER3)
    RemoteInfinispanServer server3;

    @ArquillianResource
    ContainerController controller;

    protected final List<MBeanServerConnectionProvider> providers = new ArrayList<MBeanServerConnectionProvider>();
    private RemoteCacheManager rcm1;
    private RemoteCacheManager rcm2;
    private RemoteCache cache1;
    private RemoteCache cache2;

    private MemcachedClient mc;

    @Before
    public void setUp() throws Exception {
        // clear list of providers before test
        providers.clear();

        providers.add(new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT));
        providers.add(new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER2_MGMT_PORT));
        providers.add(new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER3_MGMT_PORT));

        // hotrod
        rcm1 = ITestUtils.createCacheManager(server1);
        rcm2 = ITestUtils.createCacheManager(server2);
        cache1 = rcm1.getCache(HOTROD_CACHE_NAME);
        cache2 = rcm2.getCache(HOTROD_CACHE_NAME);

        // memcached
        try {
            mc = new MemcachedClient("UTF-8", server1.getMemcachedEndpoint().getInetAddress()
                    .getHostName(), server1.getMemcachedEndpoint().getPort(), server1.getMemcachedEndpoint().getPort());
        } catch (Exception ex) {
            log.warn("prepare() method throws exception", ex);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (null != rcm1)
            rcm1.stop();
        if (null != rcm2)
            rcm2.stop();
    }

    protected MBeanServerConnectionProvider provider(int index) {
        return providers.get(index);
    }

    private String getCacheManagerName() {
        return CACHE_MANAGER_NAME;
    }

    private long numEntries(RemoteInfinispanServer server, String cacheName) {
        return server.getCacheManager(getCacheManagerName()).getCache(cacheName).getNumberOfEntries();
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER1), @RunningServer(name = CONTAINER2)})
    public void testRebalanceWithFirstNodeStop() throws Exception {
        try {
            verifyRebalanceWith3rdNode();

            //Disabling rebalance.
            setAttribute(provider(0), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, false);
            controller.stop(CONTAINER1);
            checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE2_NODE3}, OWNERS_2_MEMBERS_NODE2_NODE3, provider(1), provider(2));
            checkRebalanceStatus(false, provider(1), provider(2));
            assertTrue(numEntries(server2, HOTROD_CACHE_NAME) < NUMBER_ENTRIES);
            assertTrue(numEntries(server3, HOTROD_CACHE_NAME) < NUMBER_ENTRIES);
            assertTrue(numEntries(server2, MEMCACHED_CACHE_NAME) < NUMBER_ENTRIES);
            assertTrue(numEntries(server3, MEMCACHED_CACHE_NAME) < NUMBER_ENTRIES);

            //Enabling rebalance
            setAttribute(provider(1), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, true);
            checkRebalanceStatus(true, provider(1), provider(2));

            checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE2_NODE3}, OWNERS_2_MEMBERS_NODE2_NODE3, provider(1), provider(2));

            assertTrue(numEntries(server2, HOTROD_CACHE_NAME) == NUMBER_ENTRIES);
            assertTrue(numEntries(server3, HOTROD_CACHE_NAME) == NUMBER_ENTRIES);
            assertTrue(numEntries(server2, MEMCACHED_CACHE_NAME) == NUMBER_ENTRIES);
            assertTrue(numEntries(server3, MEMCACHED_CACHE_NAME) == NUMBER_ENTRIES);
        } finally {
            controller.stop(CONTAINER3);
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER1), @RunningServer(name = CONTAINER2)})
    public void testRebalanceWithJoinedNodeStop() throws Exception {
        verifyRebalanceWith3rdNode();
        //Disabling rebalance.
        setAttribute(provider(0), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, false);
        controller.stop(CONTAINER3);
        checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE1_NODE2}, OWNERS_2_MEMBERS_NODE1_NODE2, provider(0), provider(1));
        checkRebalanceStatus(false, provider(0), provider(1));
        assertTrue(numEntries(server1, HOTROD_CACHE_NAME) < NUMBER_ENTRIES);
        assertTrue(numEntries(server2, HOTROD_CACHE_NAME) < NUMBER_ENTRIES);
        assertTrue(numEntries(server1, MEMCACHED_CACHE_NAME) < NUMBER_ENTRIES);
        assertTrue(numEntries(server2, MEMCACHED_CACHE_NAME) < NUMBER_ENTRIES);

        //Enabling rebalance
        setAttribute(provider(1), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, true);
        checkRebalanceStatus(true, provider(0), provider(1));

        checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE1_NODE2}, OWNERS_2_MEMBERS_NODE1_NODE2, provider(0), provider(1));

        assertEquals(NUMBER_ENTRIES, numEntries(server1, HOTROD_CACHE_NAME));
        assertEquals(NUMBER_ENTRIES, numEntries(server2, HOTROD_CACHE_NAME));
        assertEquals(NUMBER_ENTRIES, numEntries(server1, MEMCACHED_CACHE_NAME));
        assertEquals(NUMBER_ENTRIES, numEntries(server2, MEMCACHED_CACHE_NAME));
    }

    private void verifyRebalanceWith3rdNode() throws Exception {
        //Disabling Rebalance for verifying the join of the 3rd node.
        setAttribute(provider(0), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, false);

        //putting data into cache before adding new node
        putDataIntoCache(NUMBER_ENTRIES);

        //Verifying that the rebalance is disabled.
        checkRebalanceStatus(false, provider(0), provider(1));
        checkRpcManagerStatistics(new String[]{"null"}, OWNERS_2_MEMBERS_NODE1_NODE2, provider(0), provider(1));

        controller.start(CONTAINER3);

        checkRebalanceStatus(false, provider(2));

        checkRpcManagerStatistics(new String[]{"null"}, OWNERS_2_MEMBERS_NODE1_NODE2, provider(0), provider(1), provider(2));

        assertEquals("The hotrod cache on server(2) should be empty.", 0, numEntries(server3, HOTROD_CACHE_NAME));
        assertEquals("The memcached cache on server(2) should be empty.", 0, numEntries(server3, MEMCACHED_CACHE_NAME));

        //Enabling the Rebalance and verifying that the consistent rehash takes place.
        setAttribute(provider(0), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, true);
        checkRebalanceStatus(true, provider(0), provider(1), provider(2));

        checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE1_NODE2_NODE3}, null, provider(0), provider(1), provider(2));

        //Waiting for rehash take place.
        checkRpcManagerStatistics(new String[]{"null"}, OWNERS_2_MEMBERS_NODE1_NODE2_NODE3, provider(0), provider(1), provider(2));

        assertTrue(numEntries(server1, HOTROD_CACHE_NAME) < NUMBER_ENTRIES);
        assertTrue(numEntries(server2, HOTROD_CACHE_NAME) < NUMBER_ENTRIES);
        assertTrue(numEntries(server3, HOTROD_CACHE_NAME) < NUMBER_ENTRIES);
        assertTrue(numEntries(server1, MEMCACHED_CACHE_NAME) < NUMBER_ENTRIES);
        assertTrue(numEntries(server2, MEMCACHED_CACHE_NAME) < NUMBER_ENTRIES);
        assertTrue(numEntries(server3, MEMCACHED_CACHE_NAME) < NUMBER_ENTRIES);
    }

    private void checkRebalanceStatus(final boolean expectedStatus, MBeanServerConnectionProvider... providers) throws Exception {
        for (final MBeanServerConnectionProvider provider : providers) {
            eventually(new ITestUtils.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return expectedStatus == Boolean.parseBoolean(getAttribute(provider, LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME));
                }
            }, 10000);
        }
    }

    private void checkRpcManagerStatistics(String[] expectedPendingViews, final String expectedCommitedView, MBeanServerConnectionProvider... providers) throws Exception {
        // on windows, everything is slow and the view might not be yet updated, so we sleep a little
        for (final MBeanServerConnectionProvider provider : providers) {
            if (expectedCommitedView != null) {
                eventually(new ITestUtils.Condition() {
                    @Override
                    public boolean isSatisfied() throws Exception {
                        String hotrodCommittedViewAsString = String.valueOf(getAttribute(provider, HOTROD_RPC_MANAGER_MBEAN, COMMITTED_VIEW_AS_STRING_ATTR_NAME));
                        String memcachedCommittedViewAsString = String.valueOf(getAttribute(provider, MEMCACHED_RPC_MANAGER_MBEAN, COMMITTED_VIEW_AS_STRING_ATTR_NAME));
                        return expectedCommitedView.equals(hotrodCommittedViewAsString) && expectedCommitedView.equals(memcachedCommittedViewAsString);
                    }
                }, 10000);
            }

            String hotrodPendingViewAsString = String.valueOf(getAttribute(provider, HOTROD_RPC_MANAGER_MBEAN, PENDING_VIEW_AS_STRING_ATTR_NAME));
            String memcachedPendingViewAsString = String.valueOf(getAttribute(provider, MEMCACHED_RPC_MANAGER_MBEAN, PENDING_VIEW_AS_STRING_ATTR_NAME));
            boolean hotrodPassed = false;
            boolean memcachedPassed = false;
            for (String expectedPendingView : expectedPendingViews) {
                if (expectedPendingView.equals(hotrodPendingViewAsString)) {
                    hotrodPassed = true;
                }
                if (expectedPendingView.equals(memcachedPendingViewAsString)) {
                    memcachedPassed = true;
                }
            }
            assertTrue("The pending view doesn't match to any of expected ones, but is " + hotrodPendingViewAsString + ".", hotrodPassed);
            assertTrue("The pending view doesn't match to any of expected ones, but is " + memcachedPendingViewAsString + ".", memcachedPassed);
        }
    }

    private void putDataIntoCache(int count) throws Exception {
        // hotrod
        for (int i = 0; i < count; i++) {
            cache1.put("key" + i, "value" + i);
        }

        assertEquals(count, cache1.size());
        assertEquals(count, cache2.size());

        // memcached
        for (int i = 0; i < count; i++) {
            mc.set("key" + i, "value" + i);
        }

        long num1 = numEntries(server1, MEMCACHED_CACHE_NAME);
        long num2 = numEntries(server2, MEMCACHED_CACHE_NAME);

        assertEquals("The size of both caches should be equal.", num1, num2);
        assertEquals(count, num1);
        assertEquals(count, num2);
    }

}
