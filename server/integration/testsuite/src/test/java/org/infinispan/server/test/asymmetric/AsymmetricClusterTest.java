package org.infinispan.server.test.asymmetric;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * A simple test for asymmetric cluster. There are two nodes in the cluster. The first one
 * has two caches defined - default and memcachedCache - both are distributed. The second node has
 * only memcachedCache defined.
 *
 * The test verifies that it is possible to store keys to both caches - without problems. The data
 * in memcachedCache is further replicated to the other node.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@RunWith(Arquillian.class)
@WithRunningServer({ "asymmetric-cluster-1", "asymmetric-cluster-2" })
public class AsymmetricClusterTest {

    @InfinispanResource("asymmetric-cluster-1")
    RemoteInfinispanServer server1;

    @InfinispanResource("asymmetric-cluster-2")
    RemoteInfinispanServer server2;

    MemcachedClient mc;
    MemcachedClient mc2;
    RemoteCache<String, String> hrCache;

    @Test
    public void testBasicOperations() throws Exception {
        Configuration conf = new ConfigurationBuilder().addServers(server1.getHotrodEndpoint().getInetAddress().getHostName() + ":"
                + server1.getHotrodEndpoint().getPort()).build();
        RemoteCacheManager rcm = new RemoteCacheManager(conf);
        hrCache = rcm.getCache();

        mc = new MemcachedClient(server1.getMemcachedEndpoint().getInetAddress().getHostName(), server1.getMemcachedEndpoint()
                .getPort());
        mc2 = new MemcachedClient(server2.getMemcachedEndpoint().getInetAddress().getHostName(), server2.getMemcachedEndpoint()
                .getPort());
        mc.set("k1", "v1");
        assertEquals("v1", mc.get("k1"));
        assertEquals("v1", mc2.get("k1")); //test that replication happened
        hrCache.put("k2", "v2"); //critical part
        assertEquals("v2", hrCache.get("k2"));
    }
}
