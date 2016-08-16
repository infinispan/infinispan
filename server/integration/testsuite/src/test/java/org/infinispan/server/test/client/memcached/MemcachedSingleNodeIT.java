package org.infinispan.server.test.client.memcached;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.MemcachedSingleNode;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for the Memcached client. Single node test cases.
 * The server is running standalone mode.
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({ MemcachedSingleNode.class })
public class MemcachedSingleNodeIT extends AbstractMemcachedLocalIT {

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @Override
    protected RemoteInfinispanServer getServer() {
        return server1;
    }

    @Override
    protected int getMemcachedPort() {
        return server1.getMemcachedEndpoint().getPort();
    }
}
