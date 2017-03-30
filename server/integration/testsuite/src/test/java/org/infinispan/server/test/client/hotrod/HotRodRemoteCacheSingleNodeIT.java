package org.infinispan.server.test.client.hotrod;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.HotRodClustered;
import org.infinispan.server.test.category.HotRodSingleNode;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.category.Smoke;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
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
 * Tests for the HotRod client RemoteCache class with single ISPN server.
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({SingleNode.class, Smoke.class})
public class HotRodRemoteCacheSingleNodeIT extends AbstractRemoteCacheIT {

    private static final String CACHE_TEMPLATE = "localCacheConfiguration";
    private static final String CACHE_CONTAINER = "local";

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @Deployment(testable = false, name = "filter-1")
    @TargetsContainer("container1")
    public static Archive<?> deployFilter1() {
        return createFilterArchive();
    }

    @Deployment(testable = false, name = "converter-1")
    @TargetsContainer("container1")
    public static Archive<?> deployConverter1() {
        return createConverterArchive();
    }

    @Deployment(testable = false, name = "filter-converter-1")
    @TargetsContainer("container1")
    public static Archive<?> deployFilterConverter1() {
        return createFilterConverterArchive();
    }

    @Deployment(testable = false, name = "converter-2")
    @TargetsContainer("container2")
    public static Archive<?> deployConverter2() {
        return createConverterArchive();
    }

    @Deployment(testable = false, name = "filter-2")
    @TargetsContainer("container2")
    public static Archive<?> deployFilter2() {
        return createFilterArchive();
    }

    @Deployment(testable = false, name = "filter-converter-2")
    @TargetsContainer("container2")
    public static Archive<?> deployFilterConverter2() {
        return createFilterConverterArchive();
    }

    @Deployment(testable = false, name = "key-value-filter-converter-1")
    @TargetsContainer("container1")
    public static Archive<?> deployKeyValueFilterConverter1() {
        return createKeyValueFilterConverterArchive();
    }

    @Deployment(testable = false, name = "key-value-filter-converter-2")
    @TargetsContainer("container2")
    public static Archive<?> deployKeyValueFilterConverter2() {
        return createKeyValueFilterConverterArchive();
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.addCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
        client.addCache(TEST_CACHE, CACHE_CONTAINER, CACHE_TEMPLATE, ManagementClient.CacheType.LOCAL);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (remoteCacheManager != null) {
            remoteCacheManager.stop();
        }
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
