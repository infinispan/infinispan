package org.infinispan.server.test.cs.jdbc.mixed;

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests Mixed jdbc cache store under the following circumstances:
 * <p/>
 * passivation == false --all cache entries should always be also in the cache store
 * preload == true --after server restart, all entries should appear in the cache immediately
 * purge == false --all entries should remain in the cache store after server  * restart
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
public class MixedStorePassivationDisabledSinglenodeIT extends AbstractJdbcStoreSinglenodeIT {

    private final String CONFIG_MIXED_NO_PASSIVATION = "testsuite/jdbc-mixed-no-passivation.xml";
    private final String STRING_TABLE_NAME_PREFIX = "MIXED_NO_PASSIVATION_STR";
    private final String BUCKET_TABLE_NAME_PREFIX = "MIXED_NO_PASSIVATION_BKT";

    private final String CACHE_NAME = "memcachedCache";
    private final String MANAGER_NAME = "local";

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_MIXED_NO_PASSIVATION)})
    public void testFailoverWithPassivationDisabled() throws Exception {
        mc = createMemcachedClient(server);
        assertCleanCacheAndStore();

        mc.set("k1", "v1"); //store string keys
        mc.set("k2", "v2");
        dbServer.stringTable.waitForTableCreation();
        // test passivation==false, database should contain all entries which are in the cache
        System.out.println("Available tables: " + dbServer.stringTable.getTableNames());
        assertEquals(2, dbServer.stringTable.getAllRows().size());
        assertTrue(!dbServer.bucketTable.exists() || dbServer.bucketTable.getAllRows().isEmpty());
        controller.stop(CONTAINER);
        controller.start(CONTAINER);
        mc = createMemcachedClient(server);
        assertEquals("v1", mc.get("k1"));
        assertEquals("v2", mc.get("k2"));
        //when the entry is removed from the cache, it should be also removed from the cache store (the store
        //and the cache are the same sets of keys)
        mc.delete("k2");
        assertNull(dbServer.stringTable.getValueByKey("k2"));
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_MIXED_NO_PASSIVATION)})
    public void testPreloadWithoutPurge() throws Exception {
        mc = createMemcachedClient(server);
        assertCleanCacheAndStore();

        mc.set("k1", "v1"); //store string keys
        mc.set("k2", "v2");
        dbServer.stringTable.waitForTableCreation();
        System.out.println("Available tables: " + dbServer.stringTable.getTableNames());
        assertEquals(2, dbServer.stringTable.getAllRows().size());
        assertTrue(!dbServer.bucketTable.exists() || dbServer.bucketTable.getAllRows().isEmpty());
        controller.stop(CONTAINER);
        controller.start(CONTAINER);
        eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return 2 == server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries();
            }
        }, 10000);
        // test purge==false, entries should remain in the database after restart
        assertEquals(2, dbServer.stringTable.getAllRows().size());
    }

    private void assertCleanCacheAndStore() throws Exception {
        mc.delete("k1");
        mc.delete("k2");
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
