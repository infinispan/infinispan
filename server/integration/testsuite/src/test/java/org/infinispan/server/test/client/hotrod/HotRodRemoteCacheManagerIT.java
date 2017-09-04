package org.infinispan.server.test.client.hotrod;

import static org.infinispan.server.test.util.ITestUtils.isLocalMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.HotRodClustered;
import org.infinispan.server.test.category.HotRodSingleNode;
import org.infinispan.commons.test.categories.Smoke;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for the HotRod client RemoteCacheManager class in standalone mode.
 *
 * @author mgencur
 */
@RunWith(Arquillian.class)
@Category({ HotRodSingleNode.class, HotRodClustered.class, Smoke.class })
public class HotRodRemoteCacheManagerIT extends AbstractRemoteCacheManagerIT {

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @InfinispanResource("container2")
    RemoteInfinispanServer server2;

    @Override
    protected List<RemoteInfinispanServer> getServers() {
        List<RemoteInfinispanServer> servers = new ArrayList<RemoteInfinispanServer>();
        servers.add(server1);
        if (!isLocalMode()) {
            servers.add(server2);
        }
        return Collections.unmodifiableList(servers);
    }
}
