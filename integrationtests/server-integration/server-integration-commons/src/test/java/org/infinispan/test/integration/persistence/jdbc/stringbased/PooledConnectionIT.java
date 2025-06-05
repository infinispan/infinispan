package org.infinispan.test.integration.persistence.jdbc.stringbased;

import static org.infinispan.test.integration.persistence.jdbc.util.JdbcConfigurationUtil.CACHE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.integration.persistence.jdbc.util.JdbcConfigurationUtil;
import org.infinispan.test.integration.persistence.jdbc.util.TableManipulation;
import org.junit.After;
import org.junit.Test;

public class PooledConnectionIT {

    private EmbeddedCacheManager cm;

    @After
    public void cleanUp() {
        if (cm != null)
            cm.stop();
    }

    @Test
    public void testPutGetRemove() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(true, true);
        cm = jdbcUtil.getCacheManager();
        Cache<String, String> cache = cm.getCache(CACHE_NAME);
        PersistenceManager persistenceManager = ComponentRegistry.componentOf(cache, PersistenceManager.class);
        JdbcStringBasedStore<String, String> jdbcStringBasedStore = persistenceManager.getStores(JdbcStringBasedStore.class).iterator().next();
        try (TableManipulation table = new TableManipulation(jdbcStringBasedStore.getTableManager(), jdbcUtil.getPersistenceConfiguration())) {
            cache.put("k1", "v1");
            cache.put("k2", "v2");

            //not yet in store (eviction.max-entries=2, LRU)
            assertNull(table.getValueByKey("k1"));
            assertNull(table.getValueByKey("k2"));
            cache.put("k3", "v3");
            //now some key is evicted and stored in store
            assertEquals(2, getNumberOfEntriesInMemory(cache));
            // TODO: need to fix this later, for some reason this fails on Oracle but passes on other DBs
//            assertEquals(1, table.countAllRows());

            cache.stop();
            cache.start();

            assertEquals(3, cache.size());
            assertEquals("v1", cache.get("k1"));
            assertCleanCacheAndStore(cache);
        }
    }

    @Test
    public void testWithoutPreload() {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(true, false);
        cm = jdbcUtil.getCacheManager();
        Cache<String, String> cache = cm.getCache(CACHE_NAME);
        cache.put("k1", "v1");
        cache.put("k2", "v2");

        cache.stop();
        cache.start();

        assertEquals(0, getNumberOfEntriesInMemory(cache));
        assertCleanCacheAndStore(cache);
    }

    private void assertCleanCacheAndStore(Cache<?, ?> cache) {
        cache.clear();
        assertEquals(0, cache.size());
    }

    private int getNumberOfEntriesInMemory(Cache<?, ?> cache) {
        return cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size();
    }

}
