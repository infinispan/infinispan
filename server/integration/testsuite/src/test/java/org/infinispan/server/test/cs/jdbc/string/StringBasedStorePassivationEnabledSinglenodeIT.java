package org.infinispan.server.test.cs.jdbc.string;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.cs.jdbc.AbstractJdbcStoreSinglenodeIT;
import org.infinispan.server.test.util.ITestUtils.Condition;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.infinispan.server.test.util.ITestUtils.createMemcachedClient;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests String-based jdbc cache store under the following circumstances:
 * <p/>
 * passivation == true --cache entries should get to the cache store only when evicted
 * preload == false --after server restart, entries should not be loaded to the cache after server restart
 * purge == false --all entries should remain in the cache store after server restart
 * (must be false so that we can test preload)
 * <p/>
 * Other attributes like singleton, shared, fetch-state do not make sense in single node cluster.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@Category(CacheStore.class)
public class StringBasedStorePassivationEnabledSinglenodeIT extends AbstractJdbcStoreSinglenodeIT {

    private final String CONFIG_STRING_WITH_PASSIVATION = "testsuite/jdbc-string-with-passivation.xml";

    private final String TABLE_NAME_PREFIX = "STRING_WITH_PASSIVATION";
    private final String CACHE_NAME = "memcachedCache";
    private final String MANAGER_NAME = "local";

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_STRING_WITH_PASSIVATION)})
    public void testPassivateAfterEviction() throws Exception {
        mc = createMemcachedClient(server);
        assertCleanCacheAndStore();
        mc.set("k1", "v1");
        mc.set("k2", "v2");
        //not yet in store (eviction.max-entries=2, LRU)
        assertTrue(!dbServer.stringTable.exists() || dbServer.stringTable.getValueByKey("k1") == null);
        assertTrue(!dbServer.stringTable.exists() || dbServer.stringTable.getValueByKey("k2") == null);
        mc.set("k3", "v3");
        assertEquals("v3", mc.get("k3"));
        //now k1 evicted and stored in store
        assertTrue(2 >= server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertNotNull(dbServer.stringTable.getValueByKey("k1"));
        //retrieve from store to cache and remove from store, another key must be evicted (k2)
        mc.get("k1");
        assertTrue(2 >= server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertNull(dbServer.stringTable.getValueByKey("k1"));
        assertNotNull(dbServer.stringTable.getValueByKey("k2"));
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_STRING_WITH_PASSIVATION)})
    public void testSoftRestartWithoutPreload() throws Exception {
        mc = createMemcachedClient(server);
        assertCleanCacheAndStore();
        mc.set("k1", "v1");
        mc.set("k2", "v2");
        //not yet in store (eviction.max-entries=2, LRU)
        assertTrue(!dbServer.stringTable.exists() || dbServer.stringTable.getValueByKey("k1") == null);
        assertTrue(!dbServer.stringTable.exists() || dbServer.stringTable.getValueByKey("k2") == null);
        mc.set("k3", "v3");
        //now k1 evicted and stored in store
        assertTrue(2 >= server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertNotNull(dbServer.stringTable.getValueByKey("k1"));
        controller.stop(CONTAINER); //soft stop should store all entries from cache to store
        controller.start(CONTAINER);
        mc = createMemcachedClient(server);
        // test preload==false
        assertEquals(0, server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        // test purge==false, entries should remain in the database after restart
        assertTrue(!dbServer.stringTable.getAllRows().isEmpty());
        assertNotNull(dbServer.stringTable.getValueByKey("k1"));
        assertEquals("v1", mc.get("k1"));
    }

    /**
     * This test differs from the preceding only in calling .kill() instead of .stop()
     * and expecting 1 entry in the DB after fail-over instead of 3 when doing soft
     * restart.
     */
    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_STRING_WITH_PASSIVATION)})
    public void testFailoverWithoutPreload() throws Exception {
        mc = createMemcachedClient(server);
        assertCleanCacheAndStore();
        mc.set("k1", "v1");
        mc.set("k2", "v2");
        assertTrue(!dbServer.stringTable.exists() || dbServer.stringTable.getValueByKey("k1") == null);
        assertTrue(!dbServer.stringTable.exists() || dbServer.stringTable.getValueByKey("k2") == null);
        mc.set("k3", "v3");
        assertTrue(2 >= server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertNotNull(dbServer.stringTable.getValueByKey("k1"));
        controller.kill(CONTAINER);
        controller.start(CONTAINER);
        mc = createMemcachedClient(server);
        assertEquals(0, server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertTrue(!dbServer.stringTable.getAllRows().isEmpty());
        assertNotNull(dbServer.stringTable.getValueByKey("k1"));
        assertEquals("v1", mc.get("k1"));
    }

    private void assertCleanCacheAndStore() throws Exception {
        mc.delete("k1");
        mc.delete("k2");
        mc.delete("k3");
        if (dbServer.stringTable.exists() && !dbServer.stringTable.getAllRows().isEmpty()) {
            dbServer.stringTable.deleteAllRows();
            eventually(new Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return dbServer.stringTable.getAllRows().isEmpty();
                }
            }, 10000);
        }
    }

    @Override
    protected String bucketTableName() {
        return null;
    }

    @Override
    protected String stringTableName() {
        return TABLE_NAME_PREFIX + "_" + CACHE_NAME;
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
