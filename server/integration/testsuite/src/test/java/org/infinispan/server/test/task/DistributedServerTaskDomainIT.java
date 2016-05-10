package org.infinispan.server.test.task;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.HotRodClusteredDomain;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.infinispan.server.test.util.ITestUtils.isDistributedMode;
import static org.infinispan.server.test.util.ITestUtils.isReplicatedMode;

/**
 * Tests running the remote task execution tests in Domain mode.
 *
 * @author amanukya
 */
@RunWith(Arquillian.class)
@Category({HotRodClusteredDomain.class, Task.class})
public class DistributedServerTaskDomainIT extends AbstractDistributedServerTaskIT {
    @InfinispanResource(value = "master:server-one", jmxPort = 4447)
    RemoteInfinispanServer server1;

    @InfinispanResource(value = "master:server-two", jmxPort = 4597)
    RemoteInfinispanServer server2;

    private static final String CUSTOM_TEMPLATE_NAME = "testConf";
    private static final String CUSTOM_TX_TEMPLATE_NAME = "testConfTx";

    @Deployment(testable = false, name = "custom-distributed-task")
    @TargetsContainer("cluster")
    public static Archive<?> deploy() {
        JavaArchive jar = createJavaArchive();
        jar.addAsResource(new File("/stream_serverTask.js"));
        jar.addAsManifestResource("MANIFEST.MF");

        return jar;
    }

    @Override
    protected List<RemoteInfinispanServer> getServers() {
        List<RemoteInfinispanServer> servers = new ArrayList<RemoteInfinispanServer>();
        servers.add(server1);
        servers.add(server2);

        return Collections.unmodifiableList(servers);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        client.enableJmx();

        //Adding TX configuration & Cache with enabled compatibility
        Map<String, String> txAttrs = new HashMap<>();
        txAttrs.put("mode", "NON_XA");
        txAttrs.put("locking", "PESSIMISTIC");

        if (isDistributedMode()) {
            client.addDistributedCacheConfiguration(CUSTOM_TEMPLATE_NAME, "clustered");
            client.addDistributedCache(CACHE_NAME, "clustered", CUSTOM_TEMPLATE_NAME);
            client.enableCompatibilityForDistConfiguration(CUSTOM_TEMPLATE_NAME, "clustered");

            client.addDistributedCacheConfiguration(CUSTOM_TX_TEMPLATE_NAME, "clustered");
            client.enableTransactionForDistConfiguration(CUSTOM_TX_TEMPLATE_NAME, "clustered", txAttrs);
            client.enableCompatibilityForDistConfiguration(CUSTOM_TX_TEMPLATE_NAME, "clustered");
            client.addDistributedCache(CACHE_NAME_TX, "clustered", CUSTOM_TX_TEMPLATE_NAME);
        } else if (isReplicatedMode()) {
            client.addReplicatedCacheConfiguration(CUSTOM_TEMPLATE_NAME, "clustered");
            client.enableCompatibilityForReplConfiguration(CUSTOM_TEMPLATE_NAME, "clustered");
            client.addReplicatedCache(CACHE_NAME, "clustered", CUSTOM_TEMPLATE_NAME);

            client.addReplicatedCacheConfiguration(CUSTOM_TX_TEMPLATE_NAME, "clustered");
            client.enableTransactionForReplConfiguration(CUSTOM_TX_TEMPLATE_NAME, "clustered", txAttrs);
            client.enableCompatibilityForReplConfiguration(CUSTOM_TX_TEMPLATE_NAME, "clustered");
            client.addReplicatedCache(CACHE_NAME_TX, "clustered", CUSTOM_TX_TEMPLATE_NAME);
        }

        //@TODO The next line is a workaround for JDG-314. Please remove this line when the JDG-314 is fixed.
        client.reloadServer();

        expectedServerList = asList("master:server-two", "master:server-one");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        if (isDistributedMode()) {
            client.removeDistributedCache(CACHE_NAME, "clustered");
            client.removeDistributedCache(CACHE_NAME_TX, "clustered");
            client.removeDistributedCacheConfiguration(CUSTOM_TEMPLATE_NAME, "clustered");
            client.removeDistributedCacheConfiguration(CUSTOM_TX_TEMPLATE_NAME, "clustered");
        } else if (isReplicatedMode()) {
            client.removeReplicatedCache(CACHE_NAME, "clustered");
            client.removeReplicatedCache(CACHE_NAME_TX, "clustered");
            client.removeReplicatedCacheConfiguration(CUSTOM_TEMPLATE_NAME, "clustered");
            client.removeReplicatedCacheConfiguration(CUSTOM_TX_TEMPLATE_NAME, "clustered");
        }

        client.disableJmx();
    }
}
