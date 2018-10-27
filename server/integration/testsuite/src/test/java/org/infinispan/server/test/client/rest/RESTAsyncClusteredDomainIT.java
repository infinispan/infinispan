package org.infinispan.server.test.client.rest;

import static org.infinispan.server.test.util.ITestUtils.isReplicatedMode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.RESTClusteredDomain;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test asynchronous REST operations through a custom REST client.
 * The servers are running in domain mode.
 *
 * @author mgencur
 */
@RunWith(Arquillian.class)
@Category({ RESTClusteredDomain.class })
public class RESTAsyncClusteredDomainIT extends AbstractRESTAsyncIT {

    private static final int REST_PORT1 = 8081;
    private static final int REST_PORT2 = 8231;

    @InfinispanResource(value = "master:server-one", jmxPort = 4447)
    RemoteInfinispanServer server1;

    @InfinispanResource(value = "master:server-two", jmxPort = 4597)
    RemoteInfinispanServer server2;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        client.enableJmx();
        try {
            //do nothing in dist mode, the cache is already there
            if (isReplicatedMode()) {
                client.addSocketBinding("rest-repl", "clustered-sockets", REST_PORT1);
                client.addReplicatedCache("restCache", "clustered", "replicated");
                client.addRestEndpoint("restRepl", "clustered", "restCache", "rest-repl");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        //do nothing in dist mode, the cache is already there
        if (isReplicatedMode()) {
            client.removeRestEndpoint("restRepl");
            client.removeReplicatedCache("restCache", "clustered");
            client.removeSocketBinding("rest-repl", "clustered-sockets");
        }
        client.disableJmx();
    }

    @Override
    protected int getRestPort1() {
        return REST_PORT1;
    }

    @Override
    protected int getRestPort2() {
        return REST_PORT2;
    }

    @Override
    protected List<RemoteInfinispanServer> getServers() {
        return Collections.unmodifiableList(Arrays.asList(server1, server2));
    }
}
