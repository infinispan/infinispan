package org.infinispan.server.test.jmxmanagement.suppress.statetransfer;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.infinispan.server.test.util.TestUtil.getAttribute;
import static org.infinispan.server.test.util.TestUtil.setAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstact class for testing state transfer suppress functionality
 *
 * @author amanukya@redhat.com
 */
public abstract class AbstractStateTransferSuppressTest {

    /* container names */
    protected static final String CONTAINER1 = "suppress-state-transfer-1";
    protected static final String CONTAINER2 = "suppress-state-transfer-2";
    protected static final String CONTAINER3 = "suppress-state-transfer-3";

    /* cache MBeans */
    final String DIST_CACHE_PREFIX = "jboss.infinispan:type=Cache,name=\"" + getCacheName() + "(dist_sync)\",manager=\"" + getCacheManagerName() + "\",component=";
    final String RPC_MANAGER_MBEAN = DIST_CACHE_PREFIX + "RpcManager";

    /* JMX attribute names */
    final String REBALANCE_ENABLED_ATTR_NAME = "RebalancingEnabled";
    final String COMMITTED_VIEW_AS_STRING_ATTR_NAME = "CommittedViewAsString";
    final String PENDING_VIEW_AS_STRING_ATTR_NAME = "PendingViewAsString";

    /* JMX result views */
    private final String OWNERS_2_MEMBERS_NODE0_NODE1 = "DefaultConsistentHash{numSegments=60, numOwners=2, members=[node0/" + getCacheManagerName() + ", node1/" + getCacheManagerName() + "]}";
    private final String OWNERS_2_MEMBERS_NODE1_NODE2 = "DefaultConsistentHash{numSegments=60, numOwners=2, members=[node1/" + getCacheManagerName() + ", node2/" + getCacheManagerName() + "]}";
    private final String OWNERS_2_MEMBERS_NODE0_NODE1_NODE2 = "DefaultConsistentHash{numSegments=60, numOwners=2, members=[node0/" + getCacheManagerName() + ", node1/" + getCacheManagerName() + ", node2/" + getCacheManagerName() + "]}";

    /* server module MBeans */
    private final String LOCAL_TOPOLOGY_MANAGER = "jboss.infinispan:type=CacheManager,name=\"" + getCacheManagerName() + "\",component=LocalTopologyManager";

    /* JMX management port */
    final int managementPort = 9999;

    @InfinispanResource
    RemoteInfinispanServers serverManager;

    @ArquillianResource
    ContainerController controller;

    protected final List<MBeanServerConnectionProvider> providers = new ArrayList<MBeanServerConnectionProvider>();
    private final List<RemoteInfinispanMBeans> mbeans = new ArrayList<RemoteInfinispanMBeans>();

    @Before
    public void setUp() throws Exception {
        mbeans.clear();
        mbeans.add(RemoteInfinispanMBeans.create(serverManager, CONTAINER1, getCacheName(), getCacheManagerName()));
        mbeans.add(RemoteInfinispanMBeans.create(serverManager, CONTAINER2, getCacheName(), getCacheManagerName()));
        mbeans.add(RemoteInfinispanMBeans.create(serverManager, CONTAINER3, getCacheName(), getCacheManagerName()));

        providers.clear(); // clear list of providers before test
        prepare(); // add new providers and create other resources
    }

    @After
    public void tearDown() throws Exception {
        destroy();
    }

    protected RemoteInfinispanMBeans mbean(int index) {
        return mbeans.get(index);
    }

    protected RemoteInfinispanServer server(int index) {
        return mbean(index).server;
    }

    protected MBeanServerConnectionProvider provider(int index) {
        return providers.get(index);
    }

    @Test
    @WithRunningServer({CONTAINER1, CONTAINER2})
    public void testRebalanceSwitch() throws Exception {

        //Verifying that the rebalance is enabled by default.
        checkRebalanceStatus(true, provider(0), provider(1));

        checkRpcManagerStatistics(new String[]{"null"}, OWNERS_2_MEMBERS_NODE0_NODE1, provider(0), provider(1));

        //Disabling rebalance
        setAttribute(provider(0), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, false);
        checkRebalanceStatus(false, provider(0), provider(1));

        putDataIntoCache(100);

        checkRpcManagerStatistics(new String[]{"null"}, OWNERS_2_MEMBERS_NODE0_NODE1, provider(0), provider(1));

        //Activating the rebalance, and checking that the state transfer happens.
        setAttribute(provider(1), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, true);
        checkRebalanceStatus(true, provider(0), provider(1));

        checkRpcManagerStatistics(new String[]{"null"}, OWNERS_2_MEMBERS_NODE0_NODE1, provider(0), provider(1));
    }

    @Test
    @WithRunningServer({CONTAINER1, CONTAINER2})
    public void testRebalanceDisabledWithNewNode() throws Exception {

        try {
            verifyRebalanceWith3rdNode();
        } finally {
            controller.stop(CONTAINER3);
        }
    }

    @Test
    @WithRunningServer({CONTAINER1, CONTAINER2})
    public void testRebalanceWithFirstNodeStop() throws Exception {

        try {
            verifyRebalanceWith3rdNode();

            //Disabling rebalance.
            setAttribute(provider(0), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, false);
            controller.stop(CONTAINER1);
            checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE1_NODE2}, OWNERS_2_MEMBERS_NODE1_NODE2, provider(1), provider(2));

            Thread.sleep(5000);

            checkRebalanceStatus(false, provider(1), provider(2));

            assertTrue(server(1).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() < 100);
            assertTrue(server(2).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() < 100);

            //Enabling rebalance
            setAttribute(provider(1), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, true);
            checkRebalanceStatus(true, provider(1), provider(2));

            checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE1_NODE2}, OWNERS_2_MEMBERS_NODE1_NODE2, provider(1), provider(2));

            assertTrue(server(1).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() == 100);
            assertTrue(server(2).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() == 100);
        } finally {
            controller.stop(CONTAINER3);
        }
    }

    @Test
    @WithRunningServer({CONTAINER1, CONTAINER2})
    public void testRebalanceWithJoinedNodeStop() throws Exception {

        verifyRebalanceWith3rdNode();

        //Disabling rebalance.
        setAttribute(provider(0), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, false);
        controller.stop(CONTAINER3);
        checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE0_NODE1}, OWNERS_2_MEMBERS_NODE0_NODE1, provider(0), provider(1));

        Thread.sleep(5000);

        checkRebalanceStatus(false, provider(0), provider(1));

        assertTrue(server(0).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() < 100);
        assertTrue(server(1).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() < 100);

        //Enabling rebalance
        setAttribute(provider(1), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, true);
        checkRebalanceStatus(true, provider(0), provider(1));

        checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE0_NODE1}, OWNERS_2_MEMBERS_NODE0_NODE1, provider(0), provider(1));

        assertTrue(server(0).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() == 100);
        assertTrue(server(1).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() == 100);
    }

    private void verifyRebalanceWith3rdNode() throws Exception {
        //Disabling Rebalance for verifying the join of the 3rd node.
        setAttribute(provider(0), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, false);

        //putting data into cache before adding new node
        putDataIntoCache(100);

        //Verifying that the rebalance is disabled.
        checkRebalanceStatus(false, provider(0), provider(1));
        checkRpcManagerStatistics(new String[]{"null"}, OWNERS_2_MEMBERS_NODE0_NODE1, provider(0), provider(1));

        controller.start(CONTAINER3);
        createNewProvider();

        checkRebalanceStatus(false, provider(2));

        checkRpcManagerStatistics(new String[]{"null"}, OWNERS_2_MEMBERS_NODE0_NODE1, provider(0), provider(1), provider(2));

        assertTrue("The cache on server(2) should be empty.", server(2).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() == 0);

        //Enabling the Rebalance and verifying that the consistent rehash takes place.
        setAttribute(provider(0), LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME, true);
        checkRebalanceStatus(true, provider(0), provider(1), provider(2));

        checkRpcManagerStatistics(new String[]{"null", OWNERS_2_MEMBERS_NODE0_NODE1_NODE2}, null, provider(0), provider(1), provider(2));

        //Waiting for rehash take place.
        Thread.sleep(10000);

        checkRpcManagerStatistics(new String[]{"null"}, OWNERS_2_MEMBERS_NODE0_NODE1_NODE2, provider(0), provider(1), provider(2));

        assertTrue(server(0).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() < 100);
        assertTrue(server(1).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() < 100);
        assertTrue(server(2).getCacheManager(getCacheManagerName()).getCache(getCacheName()).getNumberOfEntries() < 100);
    }

    private void checkRebalanceStatus(final boolean expectedStatus, MBeanServerConnectionProvider... providers) throws Exception {
        for (MBeanServerConnectionProvider provider : providers) {
            assertTrue(Boolean.parseBoolean(getAttribute(provider, LOCAL_TOPOLOGY_MANAGER, REBALANCE_ENABLED_ATTR_NAME)) == expectedStatus);
        }
    }

    private void checkRpcManagerStatistics(String[] expectedPendingViews, String expectedCommitedView, MBeanServerConnectionProvider... providers) throws Exception {
        for (MBeanServerConnectionProvider provider : providers) {
            if (expectedCommitedView != null) {
                String committedViewAsString = String.valueOf(getAttribute(provider, RPC_MANAGER_MBEAN, COMMITTED_VIEW_AS_STRING_ATTR_NAME));
                assertEquals(expectedCommitedView, committedViewAsString);
            }

            String pendingViewAsString = String.valueOf(getAttribute(provider, RPC_MANAGER_MBEAN, PENDING_VIEW_AS_STRING_ATTR_NAME));
            boolean passed = false;
            for (String expectedPendingView : expectedPendingViews) {
                if (expectedPendingView.equals(pendingViewAsString)) {
                    passed = true;
                    break;
                }
            }
            assertTrue("The pending view doesn't match to any of expected ones, but is " + pendingViewAsString + ".", passed);
        }
    }

    protected abstract void prepare();

    protected abstract void destroy();

    protected abstract void putDataIntoCache(int count);

    protected abstract String getCacheName();

    protected abstract String getCacheManagerName();

    protected abstract void createNewProvider();
}
