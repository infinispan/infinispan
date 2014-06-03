package org.infinispan.server.test.expiration;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.client.rest.RESTHelper;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.client.rest.RESTHelper.fullPathKey;
import static org.infinispan.server.test.client.rest.RESTHelper.get;
import static org.infinispan.server.test.client.rest.RESTHelper.head;
import static org.infinispan.server.test.client.rest.RESTHelper.post;
import static org.infinispan.server.test.util.ITestUtils.sleepForSecs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for expiration configuration. Tested with REST (verifies JBPAPP-6928) and HotRod.
 * Memcached cache cannot be configured to use expiration, see https://bugzilla.redhat.com/show_bug.cgi?id=909177#c5 .
 * Tests when individual requests use expiration are also in client tests, here we're testing that they override the global
 * configuration.
 *
 * @author <a href="mailto:tsykora@redhat.com">Tomas Sykora</a>
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "expiration-1"),@RunningServer(name = "expiration-2")})
public class ExpirationIT {

    @InfinispanResource("expiration-1")
    RemoteInfinispanServer server1;

    @InfinispanResource("expiration-2")
    RemoteInfinispanServer server2;

    @Test
    public void testRESTExpiration() throws Exception {
        RESTHelper.addServer(server1.getRESTEndpoint().getInetAddress().getHostName(), server1.getRESTEndpoint()
                .getContextPath());
        RESTHelper.addServer(server2.getRESTEndpoint().getInetAddress().getHostName(), server2.getRESTEndpoint()
                .getContextPath());
        URI key1Path = fullPathKey(0, "k1");
        URI key2Path = fullPathKey(1, "k2");
        URI key3Path = fullPathKey(0, "k3");
        URI key4Path = fullPathKey(0, "k4");
        Assert.assertEquals(2, server1.getCacheManager("clustered").getClusterSize());
        // specific entry timeToLiveSeconds and maxIdleTimeSeconds that overrides the default
        post(key1Path, "v1", "application/text", HttpServletResponse.SC_OK, "Content-Type", "application/text",
                "timeToLiveSeconds", "3", "maxIdleTimeSeconds", "3");
        // no value means never expire
        post(key2Path, "v2", "application/text", HttpServletResponse.SC_OK, "Content-Type", "application/text");
        // 0 value means use default
        post(key3Path, "v3", "application/text", HttpServletResponse.SC_OK, "Content-Type", "application/text",
                "timeToLiveSeconds", "0", "maxIdleTimeSeconds", "0");
        post(key4Path, "v4", "application/text", HttpServletResponse.SC_OK, "Content-Type", "application/text",
                "timeToLiveSeconds", "0", "maxIdleTimeSeconds", "2");

        sleepForSecs(1);
        get(key1Path, "v1");
        get(key3Path, "v3");
        get(key4Path, "v4");
        sleepForSecs(1.1);
        // k3 and k4 expired
        get(key1Path, "v1");
        head(key3Path, HttpServletResponse.SC_NOT_FOUND);
        head(key4Path, HttpServletResponse.SC_NOT_FOUND);
        sleepForSecs(1);
        // k1 expired
        head(key1Path, HttpServletResponse.SC_NOT_FOUND);
        get(key2Path, "v2");
    }

    @Test
    public void testHotRodExpiration() throws Exception {
        RemoteCacheManager rcm1 = ITestUtils.createCacheManager(server1);
        RemoteCacheManager rcm2 = ITestUtils.createCacheManager(server2);
        RemoteCache<String, String> c = rcm1.getCache("hotrodExpiration");
        RemoteCache<String, String> c2 = rcm2.getCache("hotrodExpiration");
        // global cache lifespan - 2000ms
        c.put("key1", "value1");
        c2.put("key1_c2", "value1_c2");
        // specific entry lifespan + max-idle setting
        c.put("key2", "value2", 4000, TimeUnit.MILLISECONDS, 4000, TimeUnit.MILLISECONDS);
        c2.put("key2_c2", "value2_c2", 4000, TimeUnit.MILLISECONDS, 4000, TimeUnit.MILLISECONDS);
        assertEquals("key1 should be in cache.", "value1", c.get("key1"));
        assertEquals("key1_c2 should be in cache2.", "value1_c2", c2.get("key1_c2"));
        sleepForSecs(3);
        // entries using the global lifespan expired
        assertTrue("key1 should be expired already.", c.get("key1") == null);
        assertTrue("key1_c2 should be expired already.", c2.get("key1_c2") == null);
        assertEquals("key2 should still be in the cache.", "value2", c.get("key2"));
        assertEquals("key2_c2 should still be in the cache.", "value2_c2", c2.get("key2_c2"));
        sleepForSecs(2);
        // entries should expire from file-cache-store too
        assertTrue("key2 should be expired already.", c.get("key2") == null);
        assertTrue("key2_c2 should be expired already.", c2.get("key2_c2") == null);
    }

}
