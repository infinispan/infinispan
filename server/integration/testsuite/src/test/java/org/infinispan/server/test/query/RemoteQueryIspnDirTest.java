package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for remote queries over HotRod on a replicated cache using Infinispan directory.
 *
 * @author Adrian Nistor
 */
@RunWith(Arquillian.class)
@WithRunningServer("remote-query-infinispan-dir")
public class RemoteQueryIspnDirTest extends RemoteQueryTest {

    @InfinispanResource("remote-query-infinispan-dir")
    private RemoteInfinispanServer server;

    public RemoteQueryIspnDirTest() {
       cacheContainerName = "clustered";
    }

    @Override
    protected RemoteInfinispanServer getServer() {
        return server;
    }
}
