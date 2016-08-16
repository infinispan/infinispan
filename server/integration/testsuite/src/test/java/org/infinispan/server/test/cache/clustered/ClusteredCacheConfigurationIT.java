package org.infinispan.server.test.cache.clustered;

import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test queue-size, queue-flush-interval, remote-timeout and owners attributes of a cache.
 * Please note that the queue related attributes are only aplicable to replicated-cache and remote-timeout only for SYNC mode.
 * Using hotrod client except for the queue-flush-interval, where it's more convenient to use memcached.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @author <a href="mailto:jmarkos@redhat.com">Jakub Markos</a>
 *
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name="clusteredcache-1"), @RunningServer(name = "clusteredcache-2")})
public class ClusteredCacheConfigurationIT {

    @InfinispanResource("clusteredcache-1")
    RemoteInfinispanServer server1;

    @InfinispanResource("clusteredcache-2")
    RemoteInfinispanServer server2;

    RemoteCacheManager rcm1;
    RemoteCacheManager rcm2;

    @Before
    public void setUp() {
        if (rcm1 == null) {
            Configuration conf = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName())
                    .port(server1.getHotrodEndpoint().getPort()).build();
            Configuration conf2 = new ConfigurationBuilder().addServer().host(server2.getHotrodEndpoint().getInetAddress().getHostName())
                    .port(server2.getHotrodEndpoint().getPort()).build();
            rcm1 = new RemoteCacheManager(conf);
            rcm2 = new RemoteCacheManager(conf2);
        }
    }

    @Test
    public void testReplicationTimeout() throws Exception {
        RemoteCache<String, String> rc1 = rcm1.getCache("remoteTimeoutCache");
        try {
            // create a big object
            StringBuilder sb = new StringBuilder(10000000);
            for (int i = 0; i < 1000000; i++) {
                sb.append("0123456789");
            }
            rc1.put("k1", sb.toString());
            fail("A timeout exception expected but not thrown");
        } catch (HotRodClientException he) {
            assertTrue(he.getMessage().contains("TimeoutException"));
        }
    }

    // only testing owners=1, owners=2 is tested all across the testsuite (it's also the default)
    @Test
    public void testNumOwners() throws Exception {
        RemoteCache<String, String> rc1 = rcm1.getCache("numOwners1");
        rc1.put("entry1", "value1");
        rc1.put("entry2", "value2");
        long server1Entries = server1.getCacheManager("clustered").getCache("numOwners1").getNumberOfEntries();
        long server2Entries = server2.getCacheManager("clustered").getCache("numOwners1").getNumberOfEntries();
        assertEquals(2, (server1Entries + server2Entries));
    }

    @Test
    public void testAsyncReplicationMemcached() throws Exception {
        final MemcachedClient mc1 = new MemcachedClient(server1.getMemcachedEndpoint().getInetAddress().getHostName(), server1.getMemcachedEndpoint()
                .getPort());
        final MemcachedClient mc2 = new MemcachedClient(server2.getMemcachedEndpoint().getInetAddress().getHostName(), server2.getMemcachedEndpoint()
                .getPort());
        // The write is going to happen immediately on the primary owner and asynchronously on the other node,
        // but we don't know which node is the primary because SyncConsistentHashFactory is non-deterministic.
        mc1.set("k1", "v1");
        eventually(new ITestUtils.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return mc1.get("k1") != null && mc2.get("k1") != null;
            }
        }, 1000);
        assertEquals("v1", mc1.get("k1"));
        assertEquals("v1", mc2.get("k1"));
    }

}
