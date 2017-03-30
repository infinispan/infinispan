package org.infinispan.server.test.client.hotrod;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.HotRodClustered;
import org.infinispan.server.test.category.HotRodSingleNode;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.category.Smoke;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.infinispan.server.test.util.ITestUtils.isLocalMode;

/**
 * Tests for the HotRod client RemoteCacheManager class with single ISPN server.
 *
 * @author mgencur
 */
@RunWith(Arquillian.class)
@Category({ SingleNode.class, Smoke.class })
public class HotRodRemoteCacheManagerSingleNodeIT extends AbstractRemoteCacheManagerIT {

    private static final String CACHE_TEMPLATE = "localCacheConfiguration";
    private static final String CACHE_CONTAINER = "local";

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.addCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
        client.addCache(TEST_CACHE, CACHE_CONTAINER, CACHE_TEMPLATE, ManagementClient.CacheType.LOCAL);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.removeCache(TEST_CACHE, CACHE_CONTAINER, ManagementClient.CacheType.LOCAL);
        client.removeCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
    }

    @Override
    protected List<RemoteInfinispanServer> getServers() {
        server1.reconnect();
        List<RemoteInfinispanServer> servers = new ArrayList<RemoteInfinispanServer>();
        servers.add(server1);
        return Collections.unmodifiableList(servers);
    }
}
