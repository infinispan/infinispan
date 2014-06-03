package org.infinispan.server.test.eviction;

import java.util.Arrays;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.category.Unstable;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for eviction strategy configurations
 * <p/>
 * LIRS not tested, see https://issues.jboss.org/browse/ISPN-1347
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "eviction")})
public class EvictionStrategyIT {

    @InfinispanResource("eviction")
    RemoteInfinispanServer server1;

    private static RemoteCacheManager remoteCacheManager;

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
