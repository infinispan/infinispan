package org.infinispan.it.endpoints;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test embedded caches and Hot Rod endpoints for operations that retrieve data in bulk, i.e. keySet.
 *
 * @author Martin Gencur
 * @since 7.0
 */
@Test(groups = "functional", testName = "it.endpoints.DistEmbeddedHotRodBulkTest")
public class DistEmbeddedHotRodBulkTest extends AbstractInfinispanTest {

    private final int numOwners = 1;

    private EndpointsCacheFactory<String, Integer> cacheFactory1;
    private EndpointsCacheFactory<String, Integer> cacheFactory2;

    @BeforeClass
    protected void setup() throws Exception {
        cacheFactory1 = new EndpointsCacheFactory<String, Integer>(CacheMode.DIST_SYNC, numOwners, false).setup();
        cacheFactory2 = new EndpointsCacheFactory<String, Integer>(CacheMode.DIST_SYNC, numOwners, false).setup();
    }

    @AfterClass
    protected void teardown() {
        EndpointsCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
    }

    private void populateCacheManager(BasicCache cache) {
        for (int i = 0; i < 100; i++) {
            cache.put("key" + i, i);
        }
    }

    public void testEmbeddedPutHotRodKeySet() {
        Cache<String, Integer> embedded = cacheFactory1.getEmbeddedCache();
        RemoteCache<String, Integer> remote = cacheFactory2.getHotRodCache();

        populateCacheManager(embedded);

        Set<String> keySet = remote.keySet();
        assertEquals(100, keySet.size());

        for(int i = 0; i < 100; i++) {
            assertTrue(keySet.contains("key" + i));
        }
    }
}
