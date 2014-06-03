package org.infinispan.server.test.l1;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for L1 caching.
 *
 * @author <a href="mailto:tsykora@redhat.com">Tomas Sykora</a>
 * @author <a href="mailto:wburns@redhat.com">William Burns</a>
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "l1-1"),@RunningServer(name = "l1-2")})
public class L1CachingIT {
    final String CONTAINER1 = "l1-1";
    final String CONTAINER2 = "l1-2";

    @InfinispanResource(CONTAINER1)
    RemoteInfinispanServer server1;

    @InfinispanResource(CONTAINER2)
    RemoteInfinispanServer server2;

    static final String ENCODING = "UTF-8";

    private MemcachedClient mc1;
    private MemcachedClient mc2;

    @Before
    public void setUp() throws Exception {
        mc1 = new MemcachedClient(ENCODING, server1.getMemcachedEndpoint().getInetAddress().getHostName(), server1
                .getMemcachedEndpoint().getPort(), 10000);
        mc2 = new MemcachedClient(ENCODING, server2.getMemcachedEndpoint().getInetAddress().getHostName(), server2
                .getMemcachedEndpoint().getPort(), 10000);
        mc1.delete("KeyA");
        mc2.delete("KeyA");
        mc1.delete("KeyB");
        mc2.delete("KeyB");
        mc2.delete("KeyBB");
        mc2.delete("KeyBB");
        mc1.delete("KeyC");
        mc2.delete("KeyC");
    }

    @After
    public void tearDown() throws Exception {
        if (mc1 != null) {
            mc1.close();
        }
        if (mc2 != null) {
            mc2.close();
        }
    }


    /*
    * Entries in L1 cache are stored directly into "main" cache. They are fetched from remote (distribution) node.
    * In TRACE we can see: "Doing a remote get for key KeyA" and "Caching remotely retrieved entry for key KeyA in L1"
    *
    * Number of hits is increased for cache on which is issued get. Hits on real-owner are still 0.
    * For caching L1 entries there is NO increase for number of stores. (Only for number of entries)
    */
    @Test
    public void testL1CachingEnabled() throws Exception {

        // NOTE
        // after https://issues.jboss.org/browse/ISPN-2120 (= 6.0.1 ER1)
        // I put to server1, then I issue get on server2 -> number of hits on server1 should be still 0
        // but number of hits on server2 should increase (entry was fetched from server1 to server2 for this get)
        mc1.set("KeyA", "A");
        mc1.set("KeyB", "B");
        mc1.set("KeyC", "C");

        assertTrue("Distribution of entries is wrong (at least unexpected).",
                server1.getCacheManager("clustered").getCache("memcachedCache").getNumberOfEntries() > 0);
        assertTrue("Distribution of entries is wrong (at least unexpected).",
                server2.getCacheManager("clustered").getCache("memcachedCache").getNumberOfEntries() > 0);

        assertEquals("More entries in caches than expected.", 3, server1.getCacheManager("clustered").getCache("memcachedCache").getNumberOfEntries() +
                server2.getCacheManager("clustered").getCache("memcachedCache").getNumberOfEntries());

        assertTrue("A".equals(mc2.get("KeyA")));
        assertTrue("B".equals(mc2.get("KeyB")));
        assertTrue("C".equals(mc2.get("KeyC")));

        assertEquals("Number of hits on server 1 is wrong.", 0, server1.getCacheManager("clustered").getCache("memcachedCache").getHits());
        assertEquals("Number of hits on server 2 is wrong.", 3, server2.getCacheManager("clustered").getCache("memcachedCache").getHits());

        assertEquals("Number of stores on server 1 is wrong.", 3, server1.getCacheManager("clustered").getCache("memcachedCache").getStores());
        assertEquals("Number of stores on server 2 is wrong.", 0, server2.getCacheManager("clustered").getCache("memcachedCache").getStores());

        // main condition (some entries are in the L1 cache - its copy is there)
        assertTrue("The are no entries in L1 cache! L1 seems to be disabled! Check TRACE [org.infinispan.factories.ComponentRegistry] output.",
                server1.getCacheManager("clustered").getCache("memcachedCache").getNumberOfEntries() +
                        server2.getCacheManager("clustered").getCache("memcachedCache").getNumberOfEntries() > 3);

        // *****************************
        // do the same round for server1
        assertTrue("A".equals(mc1.get("KeyA")));
        assertTrue("B".equals(mc1.get("KeyB")));
        assertTrue("C".equals(mc1.get("KeyC")));

        assertEquals("Number of hits on server 1 is wrong.", 3, server1.getCacheManager("clustered").getCache("memcachedCache").getHits());
        assertEquals("Number of hits on server 2 is wrong.", 3, server2.getCacheManager("clustered").getCache("memcachedCache").getHits());

        // should be still the same as on the start
        assertEquals("Number of stores on server 1 is wrong.", 3, server1.getCacheManager("clustered").getCache("memcachedCache").getStores());
        assertEquals("Number of stores on server 2 is wrong.", 0, server2.getCacheManager("clustered").getCache("memcachedCache").getStores());

        // main condition (there should be server1 - 1 entry + 2 in L1, server2 - 2 entries + 1 in L1 = 3x2 = 6 num of entries JMX stat.
        assertEquals("The are no entries in L1 cache! L1 seems to be disabled! Check TRACE [org.infinispan.factories.ComponentRegistry] output.",
                6, server1.getCacheManager("clustered").getCache("memcachedCache").getNumberOfEntries() +
                server2.getCacheManager("clustered").getCache("memcachedCache").getNumberOfEntries());
    }
}
