package org.infinispan.server.test.cs.jdbc.binary;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.cs.jdbc.AbstractJdbcStoreSinglenodeIT;
import org.infinispan.server.test.util.ITestUtils.Condition;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.infinispan.server.test.util.ITestUtils.getRealKeyStored;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests binary jdbc cache store under the following circumstances:
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
public class BinaryStorePassivationEnabledSinglenodeIT extends AbstractJdbcStoreSinglenodeIT {

    private final String CONFIG_BINARY_WITH_PASSIVATION = "testsuite/jdbc-binary-with-passivation.xml";
    private final String TABLE_NAME_PREFIX = "BINARY_WITH_PASSIVATION";
    private final String CACHE_NAME = "default";
    private final String MANAGER_NAME = "local";

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_BINARY_WITH_PASSIVATION)})
    public void testPassivateAfterEviction() throws Exception {
        String key1 = "key1";
        String key2 = "myBestPersonalKeyWhichHasNeverBeenBetter"; //so long to have resulting hash for the key different
        String key3 = "key3";

        cache = createCache(mbeans);
        assertCleanCacheAndStoreHotrod();

        cache.put(key1, "v1");
        cache.put(key2, "v2");
        //not yet in store (eviction.max-entries=2, LRU)
        assertTrue(!dbServer.bucketTable.exists() || dbServer.bucketTable.getAllRows().isEmpty());
        cache.put(key3, "v3");
        //now k1 evicted and stored in store
        assertTrue(2 >= server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        byte[] k1Stored = getRealKeyStored(key1, cache);
        dbServer.bucketTable.waitForTableCreation();
        assertTrue(!dbServer.bucketTable.getAllRows().isEmpty());
        assertNotNull(dbServer.bucketTable.getBucketByKey(k1Stored));
        //retrieve from store to cache and remove from store, another key must be evicted (k2)
        assertEquals("v1", cache.get(key1));
        assertTrue(2 >= server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertEquals("v2", cache.get(key2));
        assertEquals("v3", cache.get(key3));
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_BINARY_WITH_PASSIVATION)})
    public void testSoftRestartWithoutPreload() throws Exception {
        String key1 = "key1";
        String key2 = "anotherExtraUniqueKey"; //so long to have resulting hash for the key different
        String key3 = "key3";

        cache = createCache(mbeans);
        assertCleanCacheAndStoreHotrod();

        cache.put(key1, "v1");
        cache.put(key2, "v2");

        assertTrue(!dbServer.bucketTable.exists() || dbServer.bucketTable.getAllRows().isEmpty());
        cache.put(key3, "v3");

        assertTrue(2 >= server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        byte[] k1Stored = getRealKeyStored(key1, cache);
        assertTrue(!dbServer.bucketTable.getAllRows().isEmpty());
        assertNotNull(dbServer.bucketTable.getBucketByKey(k1Stored));
        controller.stop(CONTAINER); //soft stop should store all entries from cache to store
        controller.start(CONTAINER);
        cache = createCache(mbeans);
        //test preload==false, entries should be immediately in the cache after restart
        assertEquals(0, server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        //test purge==false, entries should remain in the database after restart
        assertTrue(dbServer.bucketTable.getAllRows().size() >= 2);
        assertNotNull(dbServer.bucketTable.getBucketByKey(k1Stored));
        assertEquals("v1", cache.get(key1));
    }

    /**
     * This test differs from the preceding only in calling .kill() instead of .stop()
     * and expecting 1 entry in the DB after fail-over instead of 2 when doing soft
     * restart.
     */
    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_BINARY_WITH_PASSIVATION)})
    public void testFailoverWithoutPreload() throws Exception {
        String key1 = "key1";
        String key2 = "anotherExtraUniqueKey"; //so long to have resulting hash for the key different
        String key3 = "key3";

        cache = createCache(mbeans);
        assertCleanCacheAndStoreHotrod();

        cache.put(key1, "v1");
        cache.put(key2, "v2");

        assertTrue(!dbServer.bucketTable.exists() || dbServer.bucketTable.getAllRows().isEmpty());
        cache.put(key3, "v3");

        assertTrue(2 >= server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        byte[] k1Stored = getRealKeyStored(key1, cache);
        assertTrue(!dbServer.bucketTable.getAllRows().isEmpty());
        assertNotNull(dbServer.bucketTable.getBucketByKey(k1Stored));
        controller.kill(CONTAINER);
        controller.start(CONTAINER);
        cache = createCache(mbeans);
        assertEquals(0, server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries());
        assertTrue(!dbServer.bucketTable.getAllRows().isEmpty());
        assertNotNull(dbServer.bucketTable.getBucketByKey(k1Stored));
        assertEquals("v1", cache.get(key1));
    }

    protected void assertCleanCacheAndStoreHotrod() throws Exception {
        cache.clear();
        if (dbServer.bucketTable.exists() && !dbServer.bucketTable.getAllRows().isEmpty()) {
            dbServer.bucketTable.deleteAllRows();
            eventually(new Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return dbServer.bucketTable.getAllRows().isEmpty();
                }
            }, 10000);
        }
    }

    @Override
    public String bucketTableName() {
        return TABLE_NAME_PREFIX + "_" + CACHE_NAME;
    }

    @Override
    protected String stringTableName() {
        return null;
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
