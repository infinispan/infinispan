package org.infinispan.server.test.eviction;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.category.Unstable;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for eviction strategy configurations
 * <p/>
 * LIRS not tested, see https://issues.jboss.org/browse/ISPN-1347
 */
@RunWith(Arquillian.class)
@Category(SingleNode.class)
public class EvictionStrategyIT {

    static final String CACHE_CONTAINER = "local";
    static final String CONFIG_TEMPLATE = "no-eviction-config";

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    private static RemoteCacheManager remoteCacheManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.addCacheConfiguration(CONFIG_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
        client.enableObjectEvictionForConfiguration(CACHE_CONTAINER, CONFIG_TEMPLATE, ManagementClient.CacheTemplate.LOCAL, -1);
        client.addCache("none", CACHE_CONTAINER, CONFIG_TEMPLATE, ManagementClient.CacheType.LOCAL);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.removeCache("none", CACHE_CONTAINER, ManagementClient.CacheType.LOCAL);
        client.removeCacheConfiguration(CONFIG_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
    }

    @Before
    public void setUp() {
        if (remoteCacheManager == null) {
            remoteCacheManager = ITestUtils.createCacheManager(server1);
        }
    }

    /*
     * Test for Eviction turned off
     */
    @Test
    public void testEvictionStrategyNone() {
        RemoteCache<String, String> rc = remoteCacheManager.getCache("none");
        rc.clear();
        storeKeys(rc, "A", "B", "C");
        rc.put("keyD", "D");
        assertTrue("A".equals(rc.get("keyA")));
        assertTrue("B".equals(rc.get("keyB")));
        assertTrue("C".equals(rc.get("keyC")));
        assertTrue("D".equals(rc.get("keyD")));
    }

    /*
     * Test for Eviction with LRU(Least Recently used) ordering
     */
    @Test
    @Category(Unstable.class) // See ISPN-4040
    public void testEvictionStrategyLRU() {
        RemoteCache<String, String> rc = remoteCacheManager.getCache("lru");
        rc.clear();
        storeKeys(rc, "A", "B", "C");

        assertTrue("B".equals(rc.get("keyB")));
        assertTrue("A".equals(rc.get("keyA")));

        rc.put("keyD", "D");

        assertTrue("A".equals(rc.get("keyA")));
        assertTrue("B".equals(rc.get("keyB")));
        assertTrue("D".equals(rc.get("keyD")));
        assertNull(rc.get("keyC"));
    }

    private void storeKeys(RemoteCache<String, String> rc, String... values) {
        storeKeys(rc, Arrays.asList(values));
    }

    private void storeKeys(RemoteCache<String, String> rc, List<String> values) {
        for (String value : values) {
            rc.put("key" + value, value);
        }
    }
}
