package org.infinispan.server.test.client.memcached;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.MemcachedSingleNode;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for the Memcached client. Single node test cases.
 * The server is running standalone mode.
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({SingleNode.class})
public class MemcachedSingleNodeIT extends AbstractMemcachedLocalIT {

    private static final String MEMCACHED_CACHE = "memcachedCache";
    private static final String CACHE_TEMPLATE = "localCacheConfiguration";
    private static final String CACHE_CONTAINER = "local";
    private static final String MEMCACHED_ENDPOINT = "memcached-endpoint";

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.addCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
        client.addCache(MEMCACHED_CACHE, CACHE_CONTAINER, CACHE_TEMPLATE, ManagementClient.CacheType.LOCAL);
        client.addMemcachedEndpoint(MEMCACHED_ENDPOINT, CACHE_CONTAINER, MEMCACHED_CACHE, "memcached");
        client.reloadIfRequired();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.removeMemcachedEndpoint(MEMCACHED_ENDPOINT);
        client.removeCache(MEMCACHED_CACHE, CACHE_CONTAINER, ManagementClient.CacheType.LOCAL);
        client.removeCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
    }

    @Before
    public void before() throws Exception {
        server1.reconnect();
    }

    @Override
    protected RemoteInfinispanServer getServer() {
        server1.reconnect();
        return server1;
    }

    @Override
    protected int getMemcachedPort() {
        return server1.getMemcachedEndpoint().getPort();
    }
}
