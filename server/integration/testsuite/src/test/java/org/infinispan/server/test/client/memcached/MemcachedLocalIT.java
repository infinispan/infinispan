package org.infinispan.server.test.client.memcached;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.MemcachedLocal;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;

/**
 * Tests for the Memcached client. Single node test cases.
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({ MemcachedLocal.class })
public class MemcachedLocalIT extends AbstractMemcachedLocalIT {

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
