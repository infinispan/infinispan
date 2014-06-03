package org.infinispan.server.test.cs.jdbc.multinode;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.cs.jdbc.AbstractJdbcStoreMultinodeIT;
import org.infinispan.server.test.util.ITestUtils.Condition;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.infinispan.server.test.util.ITestUtils.createMemcachedClient;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.infinispan.server.test.util.ITestUtils.startContainer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests fetch-state and singleton attributes of a string-based jdbc cache store.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@Category(CacheStore.class)
public class StringBasedStoreMultinodeIT extends AbstractJdbcStoreMultinodeIT {

    private final String CONFIG_FETCH_STATE_1 = "testsuite/jdbc-string-multinode-fetch-state1.xml";
    private final String CONFIG_FETCH_STATE_2 = "testsuite/jdbc-string-multinode-fetch-state2.xml";

    private final String CONFIG_SINGLETON_1 = "testsuite/jdbc-string-multinode-singleton1.xml";
    private final String CONFIG_SINGLETON_2 = "testsuite/jdbc-string-multinode-singleton2.xml";

    private static final String MANAGER_NAME = "clustered";
    private final String CACHE_NAME = "memcachedCache";

    private final String TABLE_NAME_PREFIX1 = "STRING_MULTINODEx";
    private final String TABLE_NAME_PREFIX2 = "STRING_MULTINODEy";

    /**
     * When a node joins a cluster, it should fetch state from other caches in the cluster and also
     * its underlying cache store should contain the same state immediately. This is ensured by fetch-state
     * attribute.
     */
    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER1, config = CONFIG_FETCH_STATE_1)})
    public void testFetchState() throws Exception {
        try {
            mc1 = createMemcachedClient(server1);
            assertCleanCacheAndStore1();
            mc1.set("k1", "v1");
            mc1.set("k2", "v2");
            mc1.set("k3", "v3");
            assertNotNull(dbServer1.stringTable.getValueByKey("k1"));
            startContainer(controller, CONTAINER2, CONFIG_FETCH_STATE_2);
            mc2 = createMemcachedClient(server2);
            assertTrue(0 < server2.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
            //the cache store should fetch state from the others
            //since eviction.max-entries==2, first k2 and k3 is loaded from the other cache, then k1 is loaded
            //from the other cache's loader and thus k2 is evicted and ends up in a cache loader
            assertNull(dbServer2.stringTable.getValueByKey("k1"));
            assertEquals("v1", mc2.get("k1"));
            assertEquals("v2", mc2.get("k2"));
            assertNull(dbServer2.stringTable.getValueByKey("k1"));
            assertNull(dbServer2.stringTable.getValueByKey("k2"));
            assertCleanCacheAndStore2();
        } finally {
            controller.stop(CONTAINER2);
        }
    }

    /**
     * The singleton attribute ensures that only a coordinator in a cluster works with
     * its underlying cache store. So even if a key is stored to a cache on server2
     * only server1 should store it into its underlying cache store as server1 is the coordinator.
     */
    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER1, config = CONFIG_SINGLETON_1)})
    public void testSingleton() throws Exception {
        try {
            mc1 = createMemcachedClient(server1);
            assertCleanCacheAndStore1();
            mc1.set("k1", "v1");
            mc1.set("k2", "v2");
            assertEquals(2, dbServer1.stringTable.getAllRows().size());
            assertNotNull(dbServer1.stringTable.getValueByKey("k1"));
            assertNotNull(dbServer1.stringTable.getValueByKey("k2"));
            startContainer(controller, CONTAINER2, CONFIG_SINGLETON_2);
            assertEquals(2, server2.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
            //the cache store should NOT fetch state from the others as there is a singleton defined
            assertTrue(dbServer2.stringTable.getAllRows().isEmpty());
            mc2 = createMemcachedClient(server2);
            mc2.set("k3", "v3");
            assertEquals(3, server1.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
            assertEquals(3, server2.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
            //keys do not go to store2 but to store1
            assertTrue(dbServer2.stringTable.getAllRows().isEmpty());
            assertEquals(3, dbServer1.stringTable.getAllRows().size());
            assertCleanCacheAndStore2();
        } finally {
            controller.stop(CONTAINER2);
        }
    }

    private void assertCleanCacheAndStore1() throws Exception {
        mc1.delete("k1");
        mc1.delete("k2");
        assertEquals(0, server1.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        if (dbServer1.stringTable.exists() && !dbServer1.stringTable.getAllRows().isEmpty()) {
            dbServer1.stringTable.deleteAllRows();
            eventually(new Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return dbServer1.stringTable.getAllRows().isEmpty();
                }
            }, 10000);
        }
        assertNull(dbServer1.stringTable.getValueByKey("k1"));
        assertNull(dbServer1.stringTable.getValueByKey("k2"));
    }

    private void assertCleanCacheAndStore2() throws Exception {
        mc2.delete("k1");
        mc2.delete("k2");
        mc2.delete("k3");
    }

    @Override
    protected void dBServers() {
        dbServer1.connectionUrl = System.getProperty("connection.url");
        dbServer1.username = System.getProperty("username");
        dbServer1.password = System.getProperty("password");
        dbServer1.bucketTableName = null;
        dbServer1.stringTableName = TABLE_NAME_PREFIX1 + "_" + CACHE_NAME;

        dbServer2.connectionUrl = System.getProperty("connection.url.other");
        dbServer2.username = System.getProperty("username.other");
        dbServer2.password = System.getProperty("password.other");
        dbServer2.bucketTableName = null;
        dbServer2.stringTableName = TABLE_NAME_PREFIX2 + "_" + CACHE_NAME;
    }

    @Override
    protected String managerName() {
        return MANAGER_NAME;
    }

    @Override
    protected String cacheName() {
        return CACHE_NAME;
    }
}
