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
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for the HotRod client RemoteCache class in standalone mode.
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({HotRodSingleNode.class, HotRodClustered.class, Smoke.class})
public class HotRodRemoteCacheIT extends AbstractRemoteCacheIT {

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @InfinispanResource("container2")
    RemoteInfinispanServer server2;

    @Deployment(testable = false, name = "filter-1")
    @TargetsContainer("container1")
    public static Archive<?> deployFilter1() {
        return createFilterArchive();
    }

    @Deployment(testable = false, name = "converter-1")
    @TargetsContainer("container1")
    public static Archive<?> deployConverter1() {
        return createConverterArchive();
    }

    @Deployment(testable = false, name = "filter-converter-1")
    @TargetsContainer("container1")
    public static Archive<?> deployFilterConverter1() {
        return createFilterConverterArchive();
    }

    @Deployment(testable = false, name = "converter-2")
    @TargetsContainer("container2")
    public static Archive<?> deployConverter2() {
        return createConverterArchive();
    }

    @Deployment(testable = false, name = "filter-2")
    @TargetsContainer("container2")
    public static Archive<?> deployFilter2() {
        return createFilterArchive();
    }

    @Deployment(testable = false, name = "filter-converter-2")
    @TargetsContainer("container2")
    public static Archive<?> deployFilterConverter2() {
        return createFilterConverterArchive();
    }

    @Deployment(testable = false, name = "key-value-filter-converter-1")
    @TargetsContainer("container1")
    public static Archive<?> deployKeyValueFilterConverter1() {
        return createKeyValueFilterConverterArchive();
    }

    @Deployment(testable = false, name = "key-value-filter-converter-2")
    @TargetsContainer("container2")
    public static Archive<?> deployKeyValueFilterConverter2() {
        return createKeyValueFilterConverterArchive();
    }

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
