package org.infinispan.server.test.client.rest;

import static org.infinispan.server.test.util.ITestUtils.isReplicatedMode;

import java.util.ArrayList;
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
 * Tests for the REST client.
 *
 * @author <a href="mailto:jvilkola@redhat.com">Jozef Vilkolak</a>
 * @author <a href="mailto:mlinhard@redhat.com">Michal Linhard</a>
 * @version August 2011
 */
@RunWith(Arquillian.class)
@Category(RESTClusteredDomain.class)
public class RESTClusteredDomainIT extends AbstractRESTClusteredIT {

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
        //do nothing in dist mode, the cache is already there
        if (isReplicatedMode()) {
            client.addSocketBinding("rest-repl", "clustered-sockets", REST_PORT1);
            client.addReplicatedCache("restReplCache", "clustered", "replicated");
            client.addRestEndpoint("restRepl", "clustered", "restReplCache", "rest-repl");
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        //do nothing in dist mode, the cache is already there
        if (isReplicatedMode()) {
            client.removeRestEndpoint("restRepl");
            client.removeReplicatedCache("restReplCache", "clustered");
            client.removeSocketBinding("rest-repl", "clustered-sockets");
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
    protected int getRestPort1() {
        return REST_PORT1;
    }

    @Override
    protected int getRestPort2() {
        return REST_PORT2;
    }
}
