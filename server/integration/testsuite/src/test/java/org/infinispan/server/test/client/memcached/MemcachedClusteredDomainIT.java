package org.infinispan.server.test.client.memcached;

import static org.infinispan.server.test.util.ITestUtils.isReplicatedMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.MemcachedClusteredDomain;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for the Memcached client. Clustered test cases.
 * The servers are running in domain mode.
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category(MemcachedClusteredDomain.class)
public class MemcachedClusteredDomainIT extends AbstractMemcachedClusteredIT {

    private static final int MEMCACHED_PORT1 = 11213;
    private static final int MEMCACHED_PORT2 = 11363;

    @InfinispanResource(value = "master:server-one", jmxPort = 4447)
    RemoteInfinispanServer server1;

    @InfinispanResource(value = "master:server-two", jmxPort = 4597)
    RemoteInfinispanServer server2;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        client.enableJmx();
        if (isReplicatedMode()) {
            client.addSocketBinding("memcached-repl", "clustered-sockets", MEMCACHED_PORT1);
            client.addReplicatedCache("memcachedReplCache", "clustered", "replicated");
            client.addMemcachedEndpoint("memcachedRepl", "clustered", "memcachedReplCache", "memcached-repl");
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        if (isReplicatedMode()) {
            client.removeMemcachedEndpoint("memcachedRepl");
            client.removeReplicatedCache("memcachedReplCache", "clustered");
            client.removeSocketBinding("memcached-repl", "clustered-sockets");
        }
        client.disableJmx();
    }

    @Override
    protected List<RemoteInfinispanServer> getServers() {
        List<RemoteInfinispanServer> servers = new ArrayList<RemoteInfinispanServer>();
        servers.add(server1);
        servers.add(server2);
        return Collections.unmodifiableList(servers);
    }

    @Override
    protected int getMemcachedPort1() {
        return MEMCACHED_PORT1;
    }

    @Override
    protected int getMemcachedPort2() {
        return MEMCACHED_PORT2;
    }

}
