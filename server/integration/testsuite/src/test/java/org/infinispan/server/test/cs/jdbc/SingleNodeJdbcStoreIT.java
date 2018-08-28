package org.infinispan.server.test.cs.jdbc;

import static org.infinispan.server.test.util.ITestUtils.createMBeans;
import static org.infinispan.server.test.util.ITestUtils.createMemcachedClient;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.infinispan.server.test.util.jdbc.DBServer;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * These are the tests for various JDBC stores (string, binary, mixed) with a single server.
 * We test each store with 2 configurations:
 * 1. passivation = true, preload = false
 * 2. passivation = false, preload = true
 * To speed things up, the tests use hotrod client so we can reuse a single server.
 * Test for the write-behind store uses memcached client.
 * Mixed store is not fully tested, because DefaultTwoWayKey2StringMapper (which does the decision string/binary) can handle
 * both string keys (memcached) and byte array keys (hotrod), which means all the keys go into the string store.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 * @author <a href="mailto:jmarkos@redhat.com">Jakub Markos</a>
 */
@RunWith(Arquillian.class)
@Category(CacheStore.class)
public class SingleNodeJdbcStoreIT {
    public static final Log log = LogFactory.getLog(SingleNodeJdbcStoreIT.class);

    public final String CONTAINER = "jdbc";

    @ArquillianResource
    protected ContainerController controller;

    @InfinispanResource(CONTAINER)
    protected RemoteInfinispanServer server;

    public final String ID_COLUMN_NAME = "id";
    public final String DATA_COLUMN_NAME = "datum";

    public static RemoteCacheManagerFactory rcmFactory;

    // WP = without passivation
    static DBServer stringDB, stringWPDB, stringAsyncDB;
    static RemoteInfinispanMBeans stringMBeans, stringWPMBeans;
    static RemoteCache stringCache, stringWPCache;

    @BeforeClass
    public static void startup() {
        rcmFactory = new RemoteCacheManagerFactory();
    }

    @AfterClass
    public static void cleanup() {
        /**
         * We need to drop the tables, because of DB2 SQL Error: SQLCODE=-204, SQLSTATE=42704
         */
        DBServer[] dbservers = {stringDB, stringWPDB, stringAsyncDB};
        for (DBServer dbServer : dbservers) {
            try {
                DBServer.TableManipulation bucketTable = dbServer.bucketTable;
                if (bucketTable != null) {
                   if (bucketTable.getConnectionUrl().contains("db2")) {
                      bucketTable.dropTable();
                      if (dbServer.stringTable != null) {
                         dbServer.stringTable.dropTable();
                      }
                   }
                }
            } catch (Exception e) {
                // catching the exception, because the drop is not part of the tests
                log.trace("Couldn't drop the tables: ", e);
            }
        }

        if (rcmFactory != null) {
            rcmFactory.stopManagers();
        }
        rcmFactory = null;
    }

    @Before
    public void setUp() throws Exception {
        if (stringDB == null) { // initialize only once (can't do it in BeforeClass because no server is running at that time)
            stringMBeans = createMBeans(server, CONTAINER, "stringWithPassivation", "local");
            stringCache = createCache(stringMBeans);
            stringDB = new DBServer(null, "STRING_WITH_PASSIVATION" + "_" + stringMBeans.cacheName, ID_COLUMN_NAME, DATA_COLUMN_NAME);

            stringWPMBeans = createMBeans(server, CONTAINER, "stringNoPassivation", "local");
            stringWPCache = createCache(stringWPMBeans);
            stringWPDB = new DBServer(null, "STRING_NO_PASSIVATION" + "_" + stringWPMBeans.cacheName, ID_COLUMN_NAME, DATA_COLUMN_NAME);

            stringAsyncDB = new DBServer(null, "STRING_ASYNC" + "_" + "memcachedCache", ID_COLUMN_NAME, DATA_COLUMN_NAME);
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER)})
    public void testNormalShutdown() throws Exception {
        // passivation = true, preload = false, purge = false, eviction.max-entries = 2 (LRU)
        testRestartStringStoreBefore();

        // passivation = false, preload = true, purge = false
        testRestartStringStoreWPBefore();

        controller.stop(CONTAINER); // normal shutdown - should store all entries from cache to store
        controller.start(CONTAINER);

        testRestartStringStoreAfter(false);

        testRestartStringStoreWPAfter();
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER)})
    public void testForcedShutdown() throws Exception {
        // passivation = true, preload = false, purge = false, eviction.max-entries = 2 (LRU)
        testRestartStringStoreBefore();

        // passivation = false, preload = true, purge = false
        testRestartStringStoreWPBefore();

        controller.kill(CONTAINER); // (kill -9)-ing the server
        controller.start(CONTAINER);

        testRestartStringStoreAfter(true);
        testRestartStringStoreWPAfter();
    }

    // simple test to see that write-behind store works
    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER)})
    public void testAsyncStringStore() throws Exception {
        MemcachedClient mc = createMemcachedClient(server);
        int numEntries = 1000;

        for (int i = 0; i != numEntries; i++) {
            mc.set("key" + i, "value" + i);
        }
        eventually(() -> stringAsyncDB.stringTable.exists(), 10000);
        for (int i = 0; i != numEntries; i++) {
            assertNotNull("key" + i + " was not found in DB in " + DBServer.TIMEOUT + " ms", stringAsyncDB.stringTable.getValueByByteArrayKeyAwait("key" + i));
        }
        for (int i = 0; i != numEntries; i++) {
            mc.delete("key" + i);
        }
        eventually(() -> stringAsyncDB.stringTable.getAllRows().isEmpty(), 10000);
    }

    public void testRestartStringStoreBefore() throws Exception {
        assertCleanCacheAndStoreHotrod(stringCache, stringDB.stringTable);
        stringCache.put("k1", "v1");
        stringCache.put("k2", "v2");
        boolean tableExists = stringDB.stringTable.exists();
        if (tableExists) {
            assertNull(stringDB.stringTable.getValueByKey(getStoredKey(stringCache, "k1")));
            assertNull(stringDB.stringTable.getValueByKey(getStoredKey(stringCache, "k2")));
        }
        stringCache.put("k3", "v3");
        //now a key would be evicted and stored in store
        assertTrue(2 >= server.getCacheManager(stringMBeans.managerName).getCache(stringMBeans.cacheName).getNumberOfEntriesInMemory());
        if (tableExists) {
            assertEquals(1, stringDB.stringTable.getAllKeys().size());
        }
    }

    public void testRestartStringStoreAfter(boolean killed) throws Exception {
        assertEquals(0, server.getCacheManager(stringMBeans.managerName).getCache(stringMBeans.cacheName).getNumberOfEntriesInMemory());

        if (killed) {
            List<String> passivatedKeys = stringDB.stringTable.getAllKeys();
            assertEquals(1, passivatedKeys.size());
            String passivatedKey = fromStoredKey(stringCache, passivatedKeys.get(0));
            assertEquals("v"+passivatedKey.substring(1), stringCache.get(passivatedKey)); // removed from store
            assertNull(stringDB.stringTable.getValueByKey(getStoredKey(stringCache, passivatedKey)));
            Set<String> allKeys = new HashSet<>(Arrays.asList("k1", "k2", "k3"));
            allKeys.remove(passivatedKey);
            for(String key : allKeys) {
                assertNull(stringCache.get(key));
            }
        } else {
            assertNotNull(stringDB.stringTable.getValueByKey(getStoredKey(stringCache, "k1")));
            assertEquals(3, stringDB.stringTable.getAllRows().size());
            assertEquals("v1", stringCache.get("k1"));
            assertEquals("v2", stringCache.get("k2"));
            assertEquals("v3", stringCache.get("k3"));
            assertNull(stringDB.stringTable.getValueByKey(getStoredKey(stringCache, "k3")));
        }
    }

    public void testRestartStringStoreWPBefore() throws Exception {
        assertCleanCacheAndStoreHotrod(stringWPCache, stringWPDB.stringTable);
        stringWPCache.put("k1", "v1");
        stringWPCache.put("k2", "v2");
        assertNotNull(stringWPDB.stringTable.getValueByKey(getStoredKey(stringWPCache, "k1")));
        assertNotNull(stringWPDB.stringTable.getValueByKey(getStoredKey(stringWPCache, "k2")));
    }

    public void testRestartStringStoreWPAfter() throws Exception {
        eventually(() -> {
            return 2 == server.getCacheManager(stringWPMBeans.managerName).getCache(stringWPMBeans.cacheName).getNumberOfEntries();
        }, 10000);
        assertEquals("v1", stringWPCache.get("k1"));
        assertEquals("v2", stringWPCache.get("k2"));
        stringWPCache.remove("k1");
        assertNull(stringWPDB.stringTable.getValueByKey(getStoredKey(stringWPCache, "k1")));
        assertNotNull(stringWPDB.stringTable.getValueByKey(getStoredKey(stringWPCache, "k2")));
    }

    public void assertCleanCacheAndStoreHotrod(RemoteCache cache, final DBServer.TableManipulation table) throws Exception {
        cache.clear();
        if (table.exists() && !table.getAllRows().isEmpty()) {
            table.deleteAllRows();
            eventually(() -> table.getAllRows().isEmpty(), 10000);
        }
    }

    // gets the database representation of the String key stored with hotrod client (when using string store)
    public String getStoredKey(RemoteCache cache, String key) throws IOException, InterruptedException {
        // 1. marshall the key
        // 2. encode it with base64 (that's what DefaultTwoWayKey2StringMapper does)
        // 3. prefix it with 8 (again, done by DefaultTwoWayKey2StringMapper to mark the key as wrapped byte array type)
        // 4. prefix it with UTF-16 BOM (that is what DefaultTwoWayKey2StringMapper does for non string values)
        return '\uFEFF' + "8" + Base64.getEncoder().encodeToString(cache.getRemoteCacheManager().getMarshaller().objectToByteBuffer(key));
    }

    public String fromStoredKey(RemoteCache cache, String key) throws IOException, InterruptedException, ClassNotFoundException {
        Object o = cache.getRemoteCacheManager().getMarshaller().objectFromByteBuffer(Base64.getDecoder().decode(key.substring(2)));
        log.tracef("Key in DB=%s > %s", key, o);
        return (String)o;
    }

    public RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans mbeans) {
        return rcmFactory.createCache(mbeans);
    }
}
