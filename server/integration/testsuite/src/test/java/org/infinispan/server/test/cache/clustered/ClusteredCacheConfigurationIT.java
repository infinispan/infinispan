package org.infinispan.server.test.cache.clustered;

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

import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.infinispan.server.test.util.ITestUtils.sleepForSecs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
            StringBuffer sb = new StringBuffer(10000000);
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

    // test queue-flush-interval=3000 (ms) with memcached
    @Test
    public void testQueueFlushIntervalMemcached() throws Exception {
        final MemcachedClient mc1 = new MemcachedClient(server1.getMemcachedEndpoint().getInetAddress().getHostName(), server1.getMemcachedEndpoint()
                .getPort());
        final MemcachedClient mc2 = new MemcachedClient(server2.getMemcachedEndpoint().getInetAddress().getHostName(), server2.getMemcachedEndpoint()
                .getPort());
        mc1.set("k1", "v1");
        assertNotNull(mc1.get("k1"));
        String value = mc2.get("k1");
        if (value == null) {
            eventually(new ITestUtils.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return mc1.get("k1") != null && mc2.get("k1") != null;
                }
            }, 3000, 10);
        } else {
            // we were unlucky - we did the put right before the flush, and did the get right after it
            // this means that the next interval window starts now and we can do another check
            mc1.set("k2", "v2");
            assertNotNull(mc1.get("k2"));
            assertNull(mc2.get("k2"));
            sleepForSecs(3.0);
            assertNotNull(mc2.get("k2"));
        }
    }

    // test queue-size=5 with hotrod
    @Test
    public void testQueueSizeHotrod() throws Exception {
        RemoteCache<String, String> rc1 = rcm1.getCache("queueSizeCache");
        long server1Entries, server2Entries;
        // do 9 puts in total, so that we know that one of the queues on server1/server2 filled up and flushed
        for (int i = 0; i < 4; i++) {
            rc1.put("k" + i, "v" + i);
        }
        // no flush could've happened yet
        server1Entries = server1.getCacheManager("clustered").getCache("queueSizeCache").getNumberOfEntries();
        server2Entries = server2.getCacheManager("clustered").getCache("queueSizeCache").getNumberOfEntries();
        long sum = server1Entries + server2Entries;
        assertTrue("There are too many entries in the cache: " + sum, sum <= 4);
        for (int i = 4; i < 9; i++) {
            rc1.put("k" + i, "v" + i);
        }
        // the replication occurs, there have to be at least 5x2 = 10 entries
        server1Entries = server1.getCacheManager("clustered").getCache("queueSizeCache").getNumberOfEntries();
        server2Entries = server2.getCacheManager("clustered").getCache("queueSizeCache").getNumberOfEntries();
        sum = server1Entries + server2Entries;
        assertTrue("The number of entries in the cache is too low: " + sum, sum >= 10);
    }
}
