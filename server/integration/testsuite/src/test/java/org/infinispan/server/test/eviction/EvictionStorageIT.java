package org.infinispan.server.test.eviction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

/**
 * Tests for eviction storage configurations
 *
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "eviction")})
public class EvictionStorageIT {

    @InfinispanResource("eviction")
    RemoteInfinispanServer server1;

    private static RemoteCacheManager remoteCacheManager;

    @Before
    public void setUp() {
        if (remoteCacheManager == null) {
            remoteCacheManager = ITestUtils.createCacheManager(server1);
        }
    }

    @Test
    public void testEvictionNone() {
        RemoteCache<String, String> rc = remoteCacheManager.getCache("none");
        rc.clear();
        storeKeys(rc, "A", "B", "C");
        rc.put("keyD", "D");
        assertEquals(4, rc.size());
        assertEquals("A",rc.get("keyA"));
        assertEquals("B", rc.get("keyB"));
        assertEquals("C", rc.get("keyC"));
        assertEquals("D", rc.get("keyD"));
    }

    @Test
    public void testBinaryStorage() {
        testEviction("binary");
    }

    @Test
    public void testObjectStorage() {
        testEviction("object");
    }

    @Test
    public void testOffHeapStorage() {
        testEviction("off-heap");
    }

    private void testEviction(String cacheName) {
        RemoteCache<String, String> rc = remoteCacheManager.getCache(cacheName);
        rc.clear();
        storeKeys(rc, "A", "B", "C");

        assertTrue("B".equals(rc.get("keyB")));
        assertTrue("A".equals(rc.get("keyA")));

        rc.put("keyD", "D");

        assertEquals(3, rc.size());
        assertEquals("D", rc.get("keyD"));
    }

    private void storeKeys(RemoteCache<String, String> rc, String... values) {
        for (String value : values) {
            rc.put("key" + value, value);
        }
    }
}
