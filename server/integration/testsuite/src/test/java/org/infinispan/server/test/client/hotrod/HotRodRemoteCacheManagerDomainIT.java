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
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
/**
 * Tests for the HotRod client RemoteCacheManager class in domain mode.
 *
 * @author Martin Gencur
 * @author Vitalii Chepeliuk
 */
@RunWith(Arquillian.class)
@Category(HotRodClusteredDomain.class)
public class HotRodRemoteCacheManagerDomainIT extends AbstractRemoteCacheManagerIT {

    @InfinispanResource(value = "master:server-one", jmxPort = 4447)
    RemoteInfinispanServer server1;

    @InfinispanResource(value = "master:server-two", jmxPort = 4597)
    RemoteInfinispanServer server2;    //when run in LOCAL mode - inject here the same container as container1

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        client.enableJmx();
        if (isDistributedMode()) {
            testCache = "cmDistTestCache";
            client.addDistributedCacheConfiguration("distCacheConfiguration", "clustered");
            client.addDistributedCache(testCache, "clustered", "distCacheConfiguration");
        } else {
            testCache = "cmReplTestCache";
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
            client.removeLocalCache(testCache, "default");
            client.removeDistributedCacheConfiguration("distCacheConfiguration", "default");
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
