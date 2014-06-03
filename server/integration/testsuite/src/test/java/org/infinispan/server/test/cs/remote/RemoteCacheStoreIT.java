package org.infinispan.server.test.cs.remote;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.category.Unstable;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests remote cache store under the following circumstances:
 * <p/>
 * passivation == true --cache entries should get to the remote cache store only when evicted
 * preload == false --after server restart, entries should be be preloaded to the cache
 * purge == false --all entries should remain in the cache store after server restart
 * (must be false so that we can test preload)
 * <p/>
 * Other attributes like singleton, shared, fetch-state do not make sense in single node cluster.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @author <a href="mailto:tsykora@redhat.com">Tomas Sykora</a>
 */
@RunWith(Arquillian.class)
@Category(CacheStore.class)
@WithRunningServer({@RunningServer(name = "standalone-rcs-remote")})
public class RemoteCacheStoreIT {

    private final String CONTAINER_LOCAL = "standalone-rcs-local"; // manual container
    private final String CONTAINER_REMOTE = "standalone-rcs-remote"; // suite container
    public static final String LOCAL_CACHE_MANAGER = "local";
    private final String LOCAL_CACHE_NAME = "memcachedCache";
    private final String READONLY_CACHE_NAME = "readOnlyCache";
    private final String HOTROD_CACHE_NAME = "hotrodCache";

    private MemcachedClient mc;
    RemoteCache<Object, Object> cache;

    @InfinispanResource(CONTAINER_LOCAL)
    RemoteInfinispanServer server1;

    @InfinispanResource(CONTAINER_REMOTE)
    RemoteInfinispanServer server2;

    @ArquillianResource
    ContainerController controller;

    RemoteCacheManager rcm1;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new ConfigurationBuilder().addServer().host(server2.getHotrodEndpoint().getInetAddress().getHostName()).port(server2
                .getHotrodEndpoint().getPort()).build();
        cache = new RemoteCacheManager(conf).getCache();
    }

    /**
     * Test for read-only attribute of store - if true, no entries will be written into store
     */
    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER_LOCAL)})
    public void testReadOnly() throws Exception {
        Configuration conf = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName()).port(server1
                .getHotrodEndpoint().getPort()).build();
        rcm1 = new RemoteCacheManager(conf);
        RemoteCache<String, String> rc1 = rcm1.getCache(READONLY_CACHE_NAME);
        // put 3 keys, k1 is evicted, but not stored
        rc1.put("k1", "v1");
        rc1.put("k2", "v2");
        rc1.put("k3", "v3");
        assertEquals(0, server2.getCacheManager(LOCAL_CACHE_MANAGER).getDefaultCache().getNumberOfEntries());
        assertEquals(2, server1.getCacheManager(LOCAL_CACHE_MANAGER).getCache(READONLY_CACHE_NAME).getNumberOfEntries());
        assertNull(rc1.get("k1"));
        assertEquals("v2", rc1.get("k2"));
        assertEquals("v3", rc1.get("k3"));
    }

    /*
     * 1. store 3 entries in the local cache 
     * 2. verify that there are only 2 in the local cache (third one evicted) 
     * 3. verify the evicted entry (and not anything else) is in the remote cache 
     * 4. retrieve the evicted entry from local cache (should call remote cache internally) 
     * 5. verify the evicted entry was removed from the remote cache
     */
    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER_LOCAL)})
    public void testPassivateAfterEviction() throws Exception {
        mc = new MemcachedClient(server1.getMemcachedEndpoint().getInetAddress().getHostName(), server1.getMemcachedEndpoint()
                .getPort());
        assertCleanCacheAndStore();
        mc.set("k1", "v1");
        mc.set("k2", "v2");
        // not yet in store (eviction.max-entries=2, LRU)
        assertEquals(0, server2.getCacheManager(LOCAL_CACHE_MANAGER).getDefaultCache().getNumberOfEntries());
        mc.set("k3", "v3");
        // now k1 evicted and stored in store
        assertEquals(2, server1.getCacheManager(LOCAL_CACHE_MANAGER).getCache(LOCAL_CACHE_NAME).getNumberOfEntries());
        assertEquals(1, server2.getCacheManager(LOCAL_CACHE_MANAGER).getDefaultCache().getNumberOfEntries());
        // retrieve from store to cache and remove from store, another key must be evicted (k2)
        assertEquals("v1", mc.get("k1"));
        assertEquals("v2", mc.get("k2"));
        assertEquals("v3", mc.get("k3"));
        mc.delete("k1");
        mc.delete("k2");
        mc.delete("k3");
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER_LOCAL)})
    @Category(Unstable.class)
    public void testSocketTimeoutForRemoteStore() throws Exception {
        Configuration conf = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName()).port(server1
                .getHotrodEndpoint().getPort()).build();
        rcm1 = new RemoteCacheManager(conf);
        RemoteCache<String, String> rc1 = rcm1.getCache(HOTROD_CACHE_NAME);
        try {
            // clear remote store and default local cache
            rcm1.getCache(HOTROD_CACHE_NAME).clear();
            // create big object
            StringBuffer sb = new StringBuffer(30000000);
            for (int i = 0; i < 3000000; i++) {
                sb.append("0123456789");
            }
            assertEquals(0, server1.getCacheManager(LOCAL_CACHE_MANAGER).getCache(HOTROD_CACHE_NAME).getNumberOfEntries());
            rc1.put("k1", sb.toString());
            rc1.put("k2", sb.toString());
            assertEquals(sb.toString(), rc1.get("k1"));
            assertEquals(sb.toString(), rc1.get("k2"));
            assertEquals(2, server1.getCacheManager(LOCAL_CACHE_MANAGER).getCache(HOTROD_CACHE_NAME).getNumberOfEntries());
            // eviction to remote cache
            // socket-timeout="1" (0 means infinite value!)
            rc1.put("k3", sb.toString());
            fail("Socket timeout for remote store was set to 1 millis and so a SocketTimeoutException was expected but not thrown.");
        } catch (Exception e) {
            // ok
            assertTrue(e.getMessage(), e.getMessage().contains("SocketTimeoutException"));
        } finally {
            controller.kill(CONTAINER_LOCAL);
        }
    }

    private void assertCleanCacheAndStore() throws Exception {
        mc.delete("k1");
        mc.delete("k2");
        mc.delete("k3");
        cache.clear();
        assertEquals(0, server1.getCacheManager(LOCAL_CACHE_MANAGER).getCache(LOCAL_CACHE_NAME).getNumberOfEntries());
        assertEquals(0, server2.getCacheManager(LOCAL_CACHE_MANAGER).getDefaultCache().getNumberOfEntries());
    }
}
