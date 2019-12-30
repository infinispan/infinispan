package org.infinispan.server.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.category.Persistence;
import org.infinispan.server.test.junit4.DatabaseServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests String-based jdbc cache store under the following circumstances:
 * <p>
 * passivation == false --all cache entries should always be also in the cache store
 * preload == true --after server restart, all entries should appear in the cache immediately
 * purge == false --all entries should remain in the cache store after server  * restart
 * <p>
 * Other attributes like singleton, shared, fetch-state do not make sense in single node cluster.
 *
 */
@Category(Persistence.class)
@RunWith(Parameterized.class)
public class JdbcStringBasedCacheStorePassivationDisabled {

    @ClassRule
    public static InfinispanServerRule SERVER = PersistenceIT.SERVERS;

    @ClassRule
    public static DatabaseServerRule DATABASE = new DatabaseServerRule(SERVER);

    @Rule
    public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        String[] databaseTypes = DatabaseServerRule.getDatabaseTypes("h2");
        List<Object[]> params = new ArrayList<>(databaseTypes.length);
        for (String databaseType : databaseTypes) {
            params.add(new Object[]{databaseType});
        }
        return params;
    }

    public JdbcStringBasedCacheStorePassivationDisabled(String databaseType) {
        DATABASE.setDatabaseType(databaseType);
    }

    @Test(timeout = 600000)
    public void testFailoverWithPassivationDisabled() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC)
              .createPersistenceConfiguration(DATABASE, false)
              .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder());

        cache.put("k1", "v1");
        cache.put("k2", "v2");
        // test passivation==false, database should contain all entries which are in the cache

        assertNotNull(table.getValueByKey(getEncodedKey("k1")));
        assertNotNull(table.getValueByKey(getEncodedKey("k2")));

        SERVER.getServerDriver().restart(0);

        assertEquals("v1", cache.get("k1"));
        assertEquals("v2", cache.get("k2"));
        //when the entry is removed from the cache, it should be also removed from the cache store (the store
        //and the cache are the same sets of keys)
        cache.remove("k1");
        assertNull(table.getValueByKey(getEncodedKey("k1")));
        cache.clear();
    }

    @Test(timeout = 600000)
    public void testPreloadWithoutPurge() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC)
              .createPersistenceConfiguration(DATABASE, false)
              .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder());

        cache.clear();
        cache.put("k1", "v1");
        cache.put("k2", "v2");

        assertNotNull(table.getValueByKey(getEncodedKey("k1")));
        assertNotNull(table.getValueByKey(getEncodedKey("k2")));

        SERVER.getServerDriver().restart(0);

        // test preload==true, entries should be immediately in the cache after restart
        assertEquals("2", cache.size());
        // test purge==false, entries should remain in the database after restart
        assertNotNull(table.getValueByKey(getEncodedKey("k1")));
        assertNotNull(table.getValueByKey(getEncodedKey("k2")));
    }

    /*
     * This should verify that DefaultTwoWayKey2StringMapper on server side can work with ByteArrayKey which
     * is always produced by HotRod client regardless of type of key being stored in a cache.
     */
    @Test(timeout = 600000)
    public void testStoreDataWithHotRodClient() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC)
              .createPersistenceConfiguration(DATABASE, false)
              .setLockingConfigurations();
        RemoteCache<Double, Double> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder());
        Double doubleKey = 10.0;
        Double doubleValue = 20.0;

        assertEquals(0, cache.size());
        assertTrue(table.countAllRows() == 0);
        cache.put(doubleKey, doubleValue);
        // test passivation==false, database should contain all entries which are in the cache
        assertEquals(1, table.countAllRows());
        assertEquals(1, table.countAllRows());
        assertEquals(doubleValue, cache.get(doubleKey));
    }

    private String getEncodedKey(String key) {
        return Base64.getEncoder().encodeToString(key.getBytes());
    }

}
