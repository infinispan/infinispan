package org.infinispan.server.test.client.memcached;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.MemcachedClustered;
import org.infinispan.commons.test.categories.Smoke;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for the Memcached client. Clustered test cases.
 * The servers are running in standalone mode.
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({ MemcachedClustered.class, Smoke.class })
public class MemcachedClusteredIT extends AbstractMemcachedClusteredIT {

    private static final int MEMCACHED_PORT1 = 11211;
    private static final int MEMCACHED_PORT2 = 11311;

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @InfinispanResource("container2")
    RemoteInfinispanServer server2;

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
