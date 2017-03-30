package org.infinispan.server.test.client.hotrod;

import static org.infinispan.server.test.util.ITestUtils.isDistributedMode;
import static org.infinispan.server.test.util.ITestUtils.isLocalMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.HotRodClusteredDomain;
import org.infinispan.server.test.category.Smoke;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for the HotRod client RemoteCache class in domain mode.
 *
 * TODO: Run this in local mode too (by adding HotRodSingleNodeDomain.class category)
 *       Currently blocked by https://issues.jboss.org/browse/ISPN-6321
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({HotRodClusteredDomain.class, Smoke.class})
public class HotRodRemoteCacheDomainIT extends AbstractRemoteCacheIT {

    @InfinispanResource(value = "master:server-one", jmxPort = 4447)
    RemoteInfinispanServer server1;

    @InfinispanResource(value = "master:server-two", jmxPort = 4597)
    RemoteInfinispanServer server2;

    @Deployment(testable = false, name = "filter-1")
    @TargetsContainer("cluster")
    public static Archive<?> deployFilter1() {
        return createFilterArchive();
    }

    @Deployment(testable = false, name = "converter-1")
    @TargetsContainer("cluster")
    public static Archive<?> deployConverter1() {
        return createConverterArchive();
    }

    @Deployment(testable = false, name = "filter-converter-1")
    @TargetsContainer("cluster")
    public static Archive<?> deployFilterConverter1() {
        return createFilterConverterArchive();
    }

    @Deployment(testable = false, name = "key-value-filter-converter-1")
    @TargetsContainer("cluster")
    public static Archive<?> deployKeyValueFilterConverter1() {
        return createKeyValueFilterConverterArchive();
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        client.enableJmx();
        if (isDistributedMode()) {
            TEST_CACHE = "distTestCache";
            client.addDistributedCacheConfiguration("distCacheConfiguration", "clustered");
            client.addDistributedCache(TEST_CACHE, "clustered", "distCacheConfiguration");
        } else if (isLocalMode()) {
            final String targetContainer = "local";
            TEST_CACHE = "localTestCache";
            client.addCacheContainer(targetContainer, TEST_CACHE);
            client.addConfigurations(targetContainer);
            client.addSocketBinding("hotrod-local", "clustered-sockets", 11223);
            client.addLocalCache(TEST_CACHE, targetContainer, "localCacheConfiguration");
            client.addHotRodEndpoint("hotrodLocal", targetContainer, TEST_CACHE, "hotrod-local");
        } else {
            TEST_CACHE = "replTestCache";
            client.addReplicatedCache(TEST_CACHE, "clustered", "replicated");
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        if (isDistributedMode()) {
            client.removeDistributedCache(TEST_CACHE, "clustered");
            client.removeDistributedCacheConfiguration("distCacheConfiguration", "clustered");
        } else if (isLocalMode()) {
            final String targetContainer = "local";
            client.removeHotRodEndpoint("hotrodLocal");
            client.removeLocalCache(TEST_CACHE, targetContainer);
            client.removeDistributedCacheConfiguration("localCacheConfiguration", "local");
            client.removeSocketBinding("hotrod-local", "clustered-sockets");
            client.removeConfigurations(targetContainer);
            client.removeCacheContainer(targetContainer);
        } else {
            client.removeReplicatedCache(TEST_CACHE, "clustered");
        }
        client.disableJmx();
    }

    @Override
    protected List<RemoteInfinispanServer> getServers() {
        List<RemoteInfinispanServer> servers = new ArrayList<RemoteInfinispanServer>();
        servers.add(server1);
        if (!isLocalMode()) {
            servers.add(server2);
        }
        return Collections.unmodifiableList(servers);
    }

}
