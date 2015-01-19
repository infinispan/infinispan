package org.infinispan.server.test.client.hotrod;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.server.test.category.HotRodClustered;
import org.infinispan.server.test.category.HotRodLocal;
import org.infinispan.server.test.category.Smoke;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OverProtocol;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests for the HotRod client RemoteCache class
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@Category({HotRodLocal.class, HotRodClustered.class, Smoke.class})
public class HotRodRemoteCacheIT extends AbstractRemoteCacheIT {

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @InfinispanResource("container2")
    RemoteInfinispanServer server2;   //when run in LOCAL mode - inject here the same container as container1

    @Deployment(testable = false, name = "filter-1")
    @TargetsContainer("container1")
    @OverProtocol("jmx-as7")
    public static Archive<?> deployFilter1() {
        return createFilterArchive();
    }

    @Deployment(testable = false, name = "converter-1")
    @TargetsContainer("container1")
    @OverProtocol("jmx-as7")
    public static Archive<?> deployConverter1() {
        return createConverterArchive();
    }

    @Deployment(testable = false, name = "converter-2")
    @TargetsContainer("container2")
    @OverProtocol("jmx-as7")
    public static Archive<?> deployConverter2() {
        return createConverterArchive();
    }

    @Deployment(testable = false, name = "filter-2")
    @TargetsContainer("container2")
    @OverProtocol("jmx-as7")
    public static Archive<?> deployFilter2() {
        return createFilterArchive();
    }

    private static Archive<?> createFilterArchive() {
        return ShrinkWrap.create(JavaArchive.class, "filter.jar")
                .addClasses(StaticCacheEventFilterFactory.class, DynamicCacheEventFilterFactory.class)
                .addAsServiceProvider(CacheEventFilterFactory.class,
                        StaticCacheEventFilterFactory.class, DynamicCacheEventFilterFactory.class);
    }

   private static Archive<?> createConverterArchive() {
      return ShrinkWrap.create(JavaArchive.class, "converter.jar")
            .addClasses(StaticCacheEventConverterFactory.class, DynamicCacheEventConverterFactory.class, CustomEvent.class)
            .addAsServiceProvider(CacheEventConverterFactory.class,
                  StaticCacheEventConverterFactory.class, DynamicCacheEventConverterFactory.class);
   }

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