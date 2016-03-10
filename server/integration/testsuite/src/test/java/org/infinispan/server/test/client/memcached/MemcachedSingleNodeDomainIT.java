package org.infinispan.server.test.client.memcached;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.MemcachedSingleNodeDomain;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for the Memcached client.
 * The server is running in domain mode.
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({ MemcachedSingleNodeDomain.class })
public class MemcachedSingleNodeDomainIT extends AbstractMemcachedLocalIT {

    private static final int MEMCACHED_PORT = 11212;

    @InfinispanResource(value = "master:server-one", jmxPort = 4447)
    RemoteInfinispanServer server1;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        client.enableJmx();
        client.addSocketBinding("memcached-local", "clustered-sockets", MEMCACHED_PORT);
        client.addLocalCache("memcachedLocalCache", "clustered", "localCacheConfiguration");
        client.addMemcachedEndpoint("memcachedLocal", "clustered", "memcachedLocalCache", "memcached-local");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        client.removeMemcachedEndpoint("memcachedLocal");
        client.removeReplicatedCache("memcachedLocalCache", "clustered");
        client.removeSocketBinding("memcached-local", "clustered-sockets");
        client.disableJmx();
    }

    @Override
    protected RemoteInfinispanServer getServer() {
        return server1;
    }

    @Override
    protected int getMemcachedPort() {
        return MEMCACHED_PORT;
    }
}
