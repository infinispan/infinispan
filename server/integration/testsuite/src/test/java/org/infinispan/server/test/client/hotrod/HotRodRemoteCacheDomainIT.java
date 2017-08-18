package org.infinispan.server.test.client.hotrod;

import static org.infinispan.server.test.util.ITestUtils.isDistributedMode;
import static org.infinispan.server.test.util.ITestUtils.isLocalMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.HotRodClusteredDomain;
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
@Category(HotRodClusteredDomain.class)
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
            testCache = "distTestCache";
            client.addDistributedCacheConfiguration("distCacheConfiguration", "clustered");
            client.addDistributedCache(testCache, "clustered", "distCacheConfiguration");
        } else if (isLocalMode()) {
            final String targetContainer = "local";
            testCache = "localTestCache";
            client.addCacheContainer(targetContainer, testCache);
            client.addConfigurations(targetContainer);
            client.addSocketBinding("hotrod-local", "clustered-sockets", 11223);
            client.addLocalCache(testCache, targetContainer, "localCacheConfiguration");
            client.addHotRodEndpoint("hotrodLocal", targetContainer, testCache, "hotrod-local");
        } else {
            testCache = "replTestCache";
            client.addReplicatedCache(testCache, "clustered", "replicated");
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        if (isDistributedMode()) {
            client.removeDistributedCache(testCache, "clustered");
            client.removeDistributedCacheConfiguration("distCacheConfiguration", "clustered");
        } else if (isLocalMode()) {
            final String targetContainer = "local";
            client.removeHotRodEndpoint("hotrodLocal");
            client.removeLocalCache(testCache, targetContainer);
            client.removeDistributedCacheConfiguration("localCacheConfiguration", "local");
            client.removeSocketBinding("hotrod-local", "clustered-sockets");
            client.removeConfigurations(targetContainer);
            client.removeCacheContainer(targetContainer);
        } else {
            client.removeReplicatedCache(testCache, "clustered");
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
