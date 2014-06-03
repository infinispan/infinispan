package org.infinispan.server.test.cs.jdbc.string;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.cs.jdbc.AbstractJdbcStoreSinglenodeIT;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.infinispan.server.test.util.ITestUtils.Condition;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.infinispan.server.test.util.ITestUtils.createMBeans;
import static org.infinispan.server.test.util.ITestUtils.createMemcachedClient;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests String-based jdbc cache store under the following circumstances:
 * <p/>
 * passivation == false --all cache entries should always be also in the cache store
 * preload == true --after server restart, all entries should appear in the cache immediately
 * purge == false --all entries should remain in the cache store after server  * restart
 * <p/>
 * Other attributes like singleton, shared, fetch-state do not make sense in single node cluster.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@Category(CacheStore.class)
public class StringBasedStorePassivationDisabledSinglenodeIT extends AbstractJdbcStoreSinglenodeIT {

    private final String CONFIG_STRING_NO_PASSIVATION = "testsuite/jdbc-string-no-passivation.xml";
    private final String TABLE_NAME_PREFIX = "STRING_NO_PASSIVATION";

    private static final String MANAGER_NAME = "local";
    private final String CACHE_NAME_STRING = "memcachedCache";
    private final String CACHE_NAME_BUCKET = "default";

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_STRING_NO_PASSIVATION)})
    public void testFailoverWithPassivationDisabled() throws Exception {
        mc = createMemcachedClient(server);
        assertCleanCacheAndStore();

        mc.set("k1", "v1");
        mc.set("k2", "v2");
        // test passivation==false, database should contain all entries which are in the cache
        assertNotNull(dbServer.stringTable.getValueByKey("k1"));
        assertNotNull(dbServer.stringTable.getValueByKey("k2"));
        controller.stop(CONTAINER);
        controller.start(CONTAINER);
        mc = createMemcachedClient(server);
        assertEquals("v1", mc.get("k1"));
        assertEquals("v2", mc.get("k2"));
        //when the entry is removed from the cache, it should be also removed from the cache store (the store
        //and the cache are the same sets of keys)
        mc.delete("k1");
        assertNull(dbServer.stringTable.getValueByKey("k1"));
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_STRING_NO_PASSIVATION)})
    public void testPreloadWithoutPurge() throws Exception {
        mc = createMemcachedClient(server);
        assertCleanCacheAndStore();

        mc.set("k1", "v1");
        mc.set("k2", "v2");
        assertNotNull(dbServer.stringTable.getValueByKey("k1"));
        assertNotNull(dbServer.stringTable.getValueByKey("k2"));
        controller.stop(CONTAINER);
        controller.start(CONTAINER);
        // test preload==true, entries should be immediately in the cache after restart
        assertEquals(2, server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME_STRING).getNumberOfEntries());
        // test purge==false, entries should remain in the database after restart
        assertNotNull(dbServer.stringTable.getValueByKey("k1"));
        assertNotNull(dbServer.stringTable.getValueByKey("k2"));
    }

    /*
     * This should verify that DefaultTwoWayKey2StringMapper on server side can work with ByteArrayKey which
     * is always produced by HotRod client regardless of type of key being stored in a cache.
     */
    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_STRING_NO_PASSIVATION)})
    public void testStoreDataWithHotRodClient() throws Exception {
        RemoteInfinispanMBeans mbeans = createMBeans(server, CONTAINER, CACHE_NAME_BUCKET, MANAGER_NAME);
        cache = createCache(mbeans);
        assertCleanCacheAndStoreHotrod();

        Double doubleKey = 10.0;
        Double doubleValue = 20.0;
        cache.clear();
        assertEquals(0, server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME_BUCKET).getNumberOfEntries());
        assertTrue(dbServer.bucketTable.getAllRows().isEmpty());
        cache.put(doubleKey, doubleValue);
        // test passivation==false, database should contain all entries which are in the cache
        assertEquals(1, dbServer.bucketTable.getAllRows().size());
        controller.stop(CONTAINER);
        controller.start(CONTAINER);
        cache = createCache(mbeans);
        assertEquals(1, dbServer.bucketTable.getAllRows().size());
        assertEquals(doubleValue, cache.get(doubleKey));
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
    protected String bucketTableName() {
        return TABLE_NAME_PREFIX + "_" + CACHE_NAME_BUCKET;
    }

    @Override
    protected String stringTableName() {
        return TABLE_NAME_PREFIX + "_" + CACHE_NAME_STRING;
    }

    @Override
    protected String managerName() {
        return MANAGER_NAME;
    }

    @Override
    protected String cacheName() {
        return CACHE_NAME_STRING;
    }


}
