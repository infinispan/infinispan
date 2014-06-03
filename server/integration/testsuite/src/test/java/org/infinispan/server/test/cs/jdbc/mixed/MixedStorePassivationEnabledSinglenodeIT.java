package org.infinispan.server.test.cs.jdbc.mixed;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.cs.jdbc.AbstractJdbcStoreSinglenodeIT;
import org.infinispan.server.test.util.ITestUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.infinispan.server.test.util.ITestUtils.createMemcachedClient;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests Mixed jdbc cache store under the following circumstances:
 * <p/>
 * passivation == true --cache entries should get to the cache store only when evicted
 * preload == false --after server restart, entries should not be loaded to the cache after server restart
 * purge == false --all entries should remain in the cache store after server restart
 * (must be false so that we can test preload)
 * <p/>
 * Other attributes like singleton, shared, fetch-state do not make sense in single node cluster.
 * <p/>
 * Mixed jdbc cache store uses both string-based and binary cache stores internally. The decision
 * into which cache store a cache entry should go is based on whether the key type can be handled
 * by DefaultTwoWayKey2StringMapper. Since this mapper can handle both String keys (when using
 * memcached client) and ByteArrayEquivalence (when using HotRod client), all keys will be stored in
 * underlying string-based cache store. This is limitation for client-server testing - we cannot
 * test that keys are stored in underlying binary cache store because we do not have any client
 * that produces keys of different type from String/ByteArrayEquivalence.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@Category(CacheStore.class)
public class MixedStorePassivationEnabledSinglenodeIT extends AbstractJdbcStoreSinglenodeIT {

    private final String CONFIG_MIXED_WITH_PASSIVATION = "testsuite/jdbc-mixed-with-passivation.xml";
    private final String STRING_TABLE_NAME_PREFIX = "MIXED_WITH_PASSIVATION_STR";
    private final String BUCKET_TABLE_NAME_PREFIX = "MIXED_WITH_PASSIVATION_BKT";

    private final String CACHE_NAME = "memcachedCache";
    private final String MANAGER_NAME = "local";

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_MIXED_WITH_PASSIVATION)})
    public void testPassivateAfterEviction() throws Exception {
        mc = createMemcachedClient(server);
        assertCleanCacheAndStore();

        mc.set("k1", "v1"); //store string keys
        mc.set("k2", "v2");
        //not yet in store (eviction.max-entries=2, LRU)
        System.out.println("Available tables: " + dbServer.stringTable.getTableNames());
        assertTrue(!dbServer.stringTable.exists() || dbServer.stringTable.getAllRows().isEmpty());
        assertTrue(!dbServer.bucketTable.exists() || dbServer.bucketTable.getAllRows().isEmpty());
        mc.set("k3", "v3");
        //now k1 evicted and stored in store
        assertEquals(2, server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertEquals(1, dbServer.stringTable.getAllRows().size());
        assertTrue(!dbServer.bucketTable.exists() || dbServer.bucketTable.getAllRows().isEmpty());
        //retrieve from store to cache and remove from store, another key must be evicted (k2)
        mc.get("k1");
        assertEquals(2, server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertNotNull(dbServer.stringTable.getValueByKey("k2"));
        assertEquals(1, dbServer.stringTable.getAllRows().size());
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_MIXED_WITH_PASSIVATION)})
    public void testFailoverWithoutPreload() throws Exception {
        mc = createMemcachedClient(server);
        assertCleanCacheAndStore();

        mc.set("k1", "v1"); //store string keys
        mc.set("k2", "v2");
        //not yet in store (eviction.max-entries=2, LRU)
        System.out.println("Available tables: " + dbServer.stringTable.getTableNames());
        assertTrue(!dbServer.stringTable.exists() || dbServer.stringTable.getAllRows().isEmpty());
        assertTrue(!dbServer.bucketTable.exists() || dbServer.bucketTable.getAllRows().isEmpty());
        //now k1 evicted and stored in store
        mc.set("k3", "v3");
        dbServer.stringTable.waitForTableCreation();
        assertTrue(0 < dbServer.stringTable.getAllRows().size());
        assertTrue(!dbServer.bucketTable.exists() || dbServer.bucketTable.getAllRows().isEmpty());
        controller.stop(CONTAINER);
        controller.start(CONTAINER);
        mc = createMemcachedClient(server);
        // test preload==false
        assertEquals(0, server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        // test purge==false, entries should remain in the database after restart
        assertEquals(3, dbServer.stringTable.getAllRows().size());
        assertTrue(dbServer.bucketTable.getAllRows().isEmpty());
        assertEquals("v1", mc.get("k1"));
    }

    private void assertCleanCacheAndStore() throws Exception {
        mc.delete("k1");
        mc.delete("k2");
        mc.delete("k3");
        if (dbServer.stringTable.exists() && !dbServer.stringTable.getAllRows().isEmpty()) {
            dbServer.stringTable.deleteAllRows();
            eventually(new ITestUtils.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return dbServer.stringTable.getAllRows().isEmpty();
                }
            }, 10000);
        }
    }

    @Override
    protected String bucketTableName() {
        return BUCKET_TABLE_NAME_PREFIX + "_" + CACHE_NAME;
    }

    @Override
    protected String stringTableName() {
        return STRING_TABLE_NAME_PREFIX + "_" + CACHE_NAME;
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
