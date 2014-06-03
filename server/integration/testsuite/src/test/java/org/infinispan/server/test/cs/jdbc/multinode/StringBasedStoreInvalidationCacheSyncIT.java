package org.infinispan.server.test.cs.jdbc.multinode;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.cs.jdbc.AbstractJdbcStoreMultinodeIT;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.infinispan.server.test.util.ITestUtils.createMemcachedClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests cache operations in Invalidation mode. There's a cluster of 2 nodes
 * backed by a common database (shared) accessed via jdbc string-based cache store.
 *
 * @author Martin Gencur
 */
@Category(CacheStore.class)
@WithRunningServer({
        @RunningServer(name = "jdbc-cachestore-1", config = "testsuite/jdbc-string-invalidation.xml"),
        @RunningServer(name = "jdbc-cachestore-2", config = "testsuite/jdbc-string-invalidation.xml")
})
public class StringBasedStoreInvalidationCacheSyncIT extends AbstractJdbcStoreMultinodeIT {

    private final String TABLE_NAME_PREFIX = "STRING_INVALIDATION";
    private final String CACHE_NAME = "memcachedCache";
    private final String MANAGER_NAME = "clustered";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        mc1 = createMemcachedClient(server1);
        mc2 = createMemcachedClient(server2);

        assertCleanCache();
    }

    @Test
    public void testResurrectEntry() throws Exception {
        mc1.set("key", "value");
        assertEquals("value", mc1.get("key"));
        assertNotNull(dbServer1.stringTable.getValueByKey("key")); //stored in DB
        assertEquals(0, server2.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries()); //not replicated
        assertEquals("value", mc2.get("key")); //load from DB

        mc1.set("key", "newValue1");
        assertEquals("newValue1", mc1.get("key"));
        assertEquals(0, server2.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries()); //invalidated/removed
        assertEquals("newValue1", mc2.get("key")); //load from DB


        mc2.set("key", "newValue2");
        assertEquals("newValue2", mc2.get("key"));
        assertEquals(0, server1.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries()); //invalidated/removed
        assertEquals("newValue2", mc1.get("key")); //load from DB
    }

    @Test
    public void testRemoveEntry() throws Exception {
        mc1.set("key", "value");
        assertEquals("value", mc1.get("key"));
        assertEquals("value", mc2.get("key")); //load via DB

        mc2.delete("key");
        assertEquals(null, mc1.get("key"));
        assertEquals(null, mc2.get("key"));
        assertEquals(null, dbServer1.stringTable.getValueByKey("key"));
    }

    @Test
    public void testRemoveNonExistentEntry() throws Exception {
        mc1.set("key", "value");
        assertEquals("value", mc1.get("key"));
        assertEquals(1, server1.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertEquals(0, server2.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries()); //not replicated

        mc2.delete("key"); //delete from the other cache (the entry is not here)
        assertEquals(null, mc1.get("key"));
        assertEquals(null, mc2.get("key"));
        assertEquals(null, dbServer1.stringTable.getValueByKey("key"));
    }

    private void assertCleanCache() throws Exception {
        mc1.delete("key");
        mc2.delete("key");
        assertEquals(0, server1.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertEquals(0, server2.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());

        assertEquals(2, server1.getCacheManager(MANAGER_NAME).getClusterSize());
        assertEquals(2, server2.getCacheManager(MANAGER_NAME).getClusterSize());
    }

    @Override
    protected void dBServers() {
        dbServer1.connectionUrl = System.getProperty("connection.url");
        dbServer1.username = System.getProperty("username");
        dbServer1.password = System.getProperty("password");
        dbServer1.bucketTableName = null;
        dbServer1.stringTableName = TABLE_NAME_PREFIX + "_" + CACHE_NAME;

        dbServer2.connectionUrl = System.getProperty("connection.url");
        dbServer2.username = System.getProperty("username");
        dbServer2.password = System.getProperty("password");
        dbServer2.bucketTableName = null;
        dbServer2.stringTableName = TABLE_NAME_PREFIX + "_" + CACHE_NAME;
    }

    protected String managerName() {
        return MANAGER_NAME;
    }

    protected String cacheName() {
        return CACHE_NAME;
    }
}