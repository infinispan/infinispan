package org.infinispan.test.integration.persistence.jdbc.stringbased;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.integration.persistence.jdbc.util.JdbcConfigurationUtil;
import org.infinispan.test.integration.persistence.jdbc.util.TableManipulation;
import org.junit.After;
import org.junit.Test;

import static org.infinispan.test.integration.persistence.jdbc.util.JdbcConfigurationUtil.CACHE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PooledConnectionIT {

    private EmbeddedCacheManager cm;

    @After
    public void cleanUp() {
        if (cm != null)
            cm.stop();
    }

    @Test
    public void testPutGetRemove() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil( true, true);
        cm = jdbcUtil.getCacheManager();
        Cache<String, String> cache = cm.getCache(CACHE_NAME);
        try(TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil)) {
            cache.put("k1", "v1");
            cache.put("k2", "v2");

            //not yet in store (eviction.max-entries=2, LRU)
            assertNull(table.getValueByKey("k1"));
            assertNull(table.getValueByKey("k2"));
            cache.put("k3", "v3");
            //now some key is evicted and stored in store
            assertEquals(2, getNumberOfEntriesInMemory(cache));
            assertEquals(1, table.countAllRows());

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

    private void assertCleanCacheAndStore(Cache cache) {
        cache.clear();
        assertEquals(0, cache.size());
    }

    private int getNumberOfEntriesInMemory(Cache<?, ?> cache) {
        return cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size();
    }

}
