package org.infinispan.server.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.category.Persistence;
import org.infinispan.server.test.core.persistence.Database;
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
public class JdbcStringBasedCacheStorePassivation {

    @ClassRule
    public static InfinispanServerRule SERVERS = PersistenceIT.SERVERS;

    @Rule
    public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

    private final Database database;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        String[] databaseTypes = PersistenceIT.DATABASE_LISTENER.getDatabaseTypes();
        List<Object[]> params = new ArrayList<>(databaseTypes.length);
        for (String databaseType : databaseTypes) {
            params.add(new Object[]{databaseType});
        }
        return params;
    }

    public JdbcStringBasedCacheStorePassivation(String databaseType) {
        this.database = PersistenceIT.DATABASE_LISTENER.getDatabase(databaseType);
    }

    @Test
    public void testFailover() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
                .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        try(TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder())) {
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            // test passivation==false, database should contain all entries which are in the cache
            assertNotNull(table.getValueByKey("k1"));
            assertNotNull(table.getValueByKey("k2"));

            SERVERS.getServerDriver().stop(0);
            SERVERS.getServerDriver().restart(0);

            assertNotNull(table.getValueByKey("k1"));
            assertNotNull(table.getValueByKey("k2"));
            assertNull(cache.withFlags(Flag.SKIP_CACHE_LOAD).get("k3"));
            assertEquals("v1", cache.get("k1"));
            assertEquals("v2", cache.get("k2"));
            //when the entry is removed from the cache, it should be also removed from the cache store (the store
            //and the cache are the same sets of keys)
            cache.remove("k1");
            assertNull(table.getValueByKey("k1"));
            cache.clear();
        }
    }

    @Test
    public void testPreload() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
                .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        try(TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder())) {
            cache.clear();
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            assertNotNull(table.getValueByKey("k1"));
            assertNotNull(table.getValueByKey("k2"));

            SERVERS.getServerDriver().stop(0);
            SERVERS.getServerDriver().restart(0);

            // test preload==true, entries should be immediately in the cache after restart
            assertEquals(2, cache.size());
            // test purge==false, entries should remain in the database after restart
            assertNotNull(table.getValueByKey("k1"));
            assertNotNull(table.getValueByKey("k2"));
        }
    }

    /*
     * This should verify that DefaultTwoWayKey2StringMapper on server side can work with ByteArrayKey which
     * is always produced by HotRod client regardless of type of key being stored in a cache.
     */
    @Test
    public void testDefaultTwoWayKey2StringMapper() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
                .setLockingConfigurations();
        RemoteCache<Object, Object> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        try(TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder())) {
            Double doubleKey = 10.0;
            Double doubleValue = 20.0;
            assertEquals(0, cache.size());
            assertEquals(0, table.countAllRows());
            cache.put(doubleKey, doubleValue);
            // test passivation==false, database should contain all entries which are in the cache
            assertEquals(1, table.countAllRows());
            assertEquals(doubleValue, cache.get(doubleKey));
        }
    }

    @Test
    public void testSoftRestartWithPassivation() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, true, false)
              .setEvition()
              .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        try(TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder())) {
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            //not yet in store (eviction.max-entries=2, LRU)
            assertNull(table.getValueByKey("k1"));
            assertNull(table.getValueByKey("k2"));
            cache.put("k3", "v3");
            //now some key is evicted and stored in store
            assertEquals(2, getNumberOfEntriesInMemory(cache));
            assertEquals(1, table.countAllRows());

            SERVERS.getServerDriver().stop(0);
            SERVERS.getServerDriver().restart(0); //soft stop should store all entries from cache to store

            // test preload==false
            assertEquals(0, getNumberOfEntriesInMemory(cache));
            // test purge==false, entries should remain in the database after restart
            assertEquals(3, table.countAllRows());
            assertEquals("v1", cache.get("k1"));
        }
    }

    /**
     * This test differs from the preceding expecting 1 entry in the DB
     * after fail-over instead of 3 when doing soft
     * restart.
     */
    @Test
    public void testFailoverWithPassivation() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, true, false)
              .setEvition()
              .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        try(TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder())) {
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            assertNull(table.getValueByKey("k1"));
            assertNull(table.getValueByKey("k2"));
            cache.put("k3", "v3");
            assertEquals(2, getNumberOfEntriesInMemory(cache));
            assertEquals(1, table.countAllRows());

            SERVERS.getServerDriver().kill(0);
            SERVERS.getServerDriver().restart(0);

            assertEquals(0, getNumberOfEntriesInMemory(cache));
            assertEquals(1, table.countAllRows());
            assertEquals("v1", cache.get("k1"));
        }
    }

    private int getNumberOfEntriesInMemory(RemoteCache<?, ?> cache) {
       return cache.withFlags(Flag.SKIP_CACHE_LOAD).size();
    }
}
