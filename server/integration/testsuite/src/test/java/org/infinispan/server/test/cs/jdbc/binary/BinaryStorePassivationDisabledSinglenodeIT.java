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

/**
 * Tests binary jdbc cache store under the following circumstances:
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
public class BinaryStorePassivationDisabledSinglenodeIT extends AbstractJdbcStoreSinglenodeIT {

    private final String TABLE_NAME_PREFIX = "BINARY_NO_PASSIVATION";  //in order to verify BZ 841896 - use slash
    private final String CACHE_NAME = "defaultx/xx";
    private final String MANAGER_NAME = "local";

    private final String CONFIG_BINARY_NO_PASSIVATION = "testsuite/jdbc-binary-no-passivation.xml";

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_BINARY_NO_PASSIVATION)})
    public void testFailoverWithPassivationDisabled() throws Exception {
        String key1 = "key1";
        String key2 = "myBestPersonalKeyWhichHasNeverBeenBetter"; //so long to have resulting hash for the key different

        cache = createCache(mbeans);
        assertCleanCacheAndStoreHotrod();

        cache.put(key1, "v1");
        cache.put(key2, "v2");
        //test passivation==false, database should contain all entries which are in the cache
        byte[] k1Stored = getRealKeyStored(key1, cache);
        byte[] k2Stored = getRealKeyStored(key2, cache);
        dbServer.bucketTable.waitForTableCreation();
        assertEquals(2, dbServer.bucketTable.getAllRows().size());
        assertNotNull(dbServer.bucketTable.getBucketByKey(k1Stored));
        assertNotNull(dbServer.bucketTable.getBucketByKey(k2Stored));
        controller.stop(CONTAINER);
        controller.start(CONTAINER);
        cache = createCache(mbeans);
        assertEquals("v1", cache.get(key1));
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_BINARY_NO_PASSIVATION)})
    public void testPreloadWithoutPurge() throws Exception {
        String key1 = "key1";
        String key2 = "myBestPersonalKeyWhichHasNeverBeenBetter"; //so long to have resulting hash for the key different

        cache = createCache(mbeans);
        assertCleanCacheAndStoreHotrod();

        cache.put(key1, "v1");
        cache.put(key2, "v2");
        byte[] k1Stored = getRealKeyStored(key1, cache);
        byte[] k2Stored = getRealKeyStored(key2, cache);
        controller.stop(CONTAINER);
        controller.start(CONTAINER);
        eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return 2 == server.getCacheManager(MANAGER_NAME).getCache(CACHE_NAME).getNumberOfEntries();
            }
        }, 10000);
        //test purge==false, entries should remain in the database after restart
        assertEquals(2, dbServer.bucketTable.getAllRows().size());
        assertNotNull(dbServer.bucketTable.getBucketByKey(k1Stored));
        assertNotNull(dbServer.bucketTable.getBucketByKey(k2Stored));
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