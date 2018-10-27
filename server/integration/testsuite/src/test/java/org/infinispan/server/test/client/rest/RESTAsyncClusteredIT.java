package org.infinispan.server.test.client.rest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.RESTClustered;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test asynchronous REST operations through a custom REST client.
 * The servers are running in standalone mode.
 *
 * @author mgencur
 */
@RunWith(Arquillian.class)
@Category({ RESTClustered.class })
public class RESTAsyncClusteredIT extends AbstractRESTAsyncIT {

    private static final int REST_PORT1 = 8080;
    private static final int REST_PORT2 = 8180;

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @InfinispanResource("container2")
    RemoteInfinispanServer server2;

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
