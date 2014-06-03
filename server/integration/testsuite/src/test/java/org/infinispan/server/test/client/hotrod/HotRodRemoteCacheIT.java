package org.infinispan.server.test.client.hotrod;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.HotRodClustered;
import org.infinispan.server.test.category.HotRodLocal;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


/**
 * Tests for the HotRod client RemoteCache class
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({ HotRodLocal.class, HotRodClustered.class })
public class HotRodRemoteCacheIT extends AbstractRemoteCacheIT {

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @InfinispanResource("container2")
    RemoteInfinispanServer server2;   //when run in LOCAL mode - inject here the same container as container1

    @Override
    protected List<RemoteInfinispanServer> getServers() {
        List<RemoteInfinispanServer> servers = new ArrayList<RemoteInfinispanServer>();
        servers.add(server1);
        if (!AbstractRemoteCacheManagerIT.isLocalMode()) {
            servers.add(server2);
        }
        return Collections.unmodifiableList(servers);
    }
}