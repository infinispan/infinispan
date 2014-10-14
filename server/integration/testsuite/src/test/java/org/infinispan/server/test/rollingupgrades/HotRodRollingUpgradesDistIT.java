package org.infinispan.server.test.rollingupgrades;

import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.server.test.category.RollingUpgradesDist;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

/**
 * Tests for rolling upgrades functionality, distribution mode.
 *
 * @author Tomas Sykora (tsykora@redhat.com)
 */
@RunWith(Arquillian.class)
@Category({RollingUpgradesDist.class})
public class HotRodRollingUpgradesDistIT {

    @InfinispanResource
    RemoteInfinispanServers serverManager;

    static final String DEFAULT_CACHE_NAME = "default";

    @ArquillianResource
    ContainerController controller;

    RemoteCacheManagerFactory rcmFactory;

    @Before
    public void setUp() {
        rcmFactory = new RemoteCacheManagerFactory();
    }

    @After
    public void tearDown() {
        if (rcmFactory != null) {
            rcmFactory.stopManagers();
        }
        rcmFactory = null;
    }

    @Test
    public void testHotRodRollingUpgradesDiffVersionsDist() throws Exception {
        // Target nodes
        final int managementPortServer1 = 9990;
        MBeanServerConnectionProvider provider1;
        // Source node
        int managementPortServer3 = 10199;
        MBeanServerConnectionProvider provider3;

        try {

            if (!Boolean.parseBoolean(System.getProperty("start.jboss.as.manually"))) {
                // start it by Arquillian
                controller.start("hotrod-rolling-upgrade-3-old-dist");
                controller.start("hotrod-rolling-upgrade-4-old-dist");
                managementPortServer3 = 10190;
            }

            // we use PROTOCOL_VERSION_12 here because older servers does not support higher versions
            ConfigurationBuilder builder3 = new ConfigurationBuilder();
            builder3.addServer()
                    .host("127.0.0.1")
                    .port(11422)
                    .protocolVersion(ConfigurationProperties.PROTOCOL_VERSION_12);

            RemoteCacheManager rcm3 = new RemoteCacheManager(builder3.build());
            final RemoteCache<String, String> c3 = rcm3.getCache("default");

            ConfigurationBuilder builder4 = new ConfigurationBuilder();
            builder4.addServer()
                    .host("127.0.0.1")
                    .port(11522)
                    .protocolVersion(ConfigurationProperties.PROTOCOL_VERSION_12);

            RemoteCacheManager rcm4 = new RemoteCacheManager(builder4.build());
            final RemoteCache<String, String> c4 = rcm4.getCache("default");

            c3.put("key1", "value1");
            assertEquals("value1", c3.get("key1"));
            c4.put("keyx1", "valuex1");
            assertEquals("valuex1", c4.get("keyx1"));

            for (int i = 0; i < 50; i++) {
                c3.put("keyLoad" + i, "valueLoad" + i);
                c4.put("keyLoadx" + i, "valueLoadx" + i);
            }

            controller.start("hotrod-rolling-upgrade-1-dist");
            controller.start("hotrod-rolling-upgrade-2-dist");

            RemoteInfinispanMBeans s1 = createRemotes("hotrod-rolling-upgrade-1-dist", "clustered-new", DEFAULT_CACHE_NAME);
            // hotrod.protocol.version, if explictily defined, is set in createCache() method
            final RemoteCache<Object, Object> c1 = createCache(s1);

            RemoteInfinispanMBeans s2 = createRemotes("hotrod-rolling-upgrade-2-dist", "clustered-new", DEFAULT_CACHE_NAME);
            final RemoteCache<Object, Object> c2 = createCache(s2);

            // test cross-fetching of entries from stores
            assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "value1", c1.get("key1"));
            assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "valuex1", c1.get("keyx1"));
            assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "value1", c2.get("key1"));
            assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "valuex1", c2.get("keyx1"));

            provider1 = new MBeanServerConnectionProvider(s1.server.getHotrodEndpoint().getInetAddress().getHostName(),
                    managementPortServer1);

            provider3 = new MBeanServerConnectionProvider("127.0.0.1", managementPortServer3);

            final ObjectName rollMan3 = new ObjectName("jboss.infinispan:type=Cache," + "name=\"default(dist_sync)\","
                    + "manager=\"clustered\"," + "component=RollingUpgradeManager");

            invokeOperation(provider3, rollMan3.toString(), "recordKnownGlobalKeyset", new Object[]{}, new String[]{});

            final ObjectName rollMan1 = new ObjectName("jboss.infinispan:type=Cache," + "name=\"default(dist_sync)\","
                    + "manager=\"clustered-new\"," + "component=RollingUpgradeManager");

            invokeOperation(provider1, rollMan1.toString(), "synchronizeData", new Object[]{"hotrod"},
                    new String[]{"java.lang.String"});

            invokeOperation(provider1, rollMan1.toString(), "disconnectSource", new Object[]{"hotrod"},
                    new String[]{"java.lang.String"});

            // is source (RemoteCacheStore) really disconnected?
            c3.put("disconnected", "source");
            c4.put("disconnectedx", "sourcex");

            assertEquals("Can't obtain value from cache3 (source node).", "source", c3.get("disconnected"));
            assertEquals("Can't obtain value from cache4 (source node).", "source", c4.get("disconnected"));
            assertEquals("Can't obtain value from cache3 (source node).", "sourcex", c3.get("disconnectedx"));
            assertEquals("Can't obtain value from cache4 (source node).", "sourcex", c4.get("disconnectedx"));

            assertNull("Source node entries should NOT be accessible from target node (after RCS disconnection)",
                    c1.get("disconnected"));
            assertNull("Source node entries should NOT be accessible from target node (after RCS disconnection)",
                    c2.get("disconnected"));
            assertNull("Source node entries should NOT be accessible from target node (after RCS disconnection)",
                    c1.get("disconnectedx"));
            assertNull("Source node entries should NOT be accessible from target node (after RCS disconnection)",
                    c2.get("disconnectedx"));

            // all entries migrated?
            assertEquals("Entry was not successfully migrated.", "value1", c1.get("key1"));
            for (int i = 0; i < 50; i++) {
                assertEquals("Entry was not successfully migrated.", "valueLoad" + i, c1.get("keyLoad" + i));
                // it is clustered, all entries should be migrated and accessible
                assertEquals("Entry was not successfully migrated.", "valueLoadx" + i, c1.get("keyLoadx" + i));
            }
        } finally {
            if (controller.isStarted("hotrod-rolling-upgrade-1-dist")) {
                controller.stop("hotrod-rolling-upgrade-1-dist");
            }
            if (controller.isStarted("hotrod-rolling-upgrade-2-dist")) {
                controller.stop("hotrod-rolling-upgrade-2-dist");
            }
            if (controller.isStarted("hotrod-rolling-upgrade-3-old-dist")) {
                controller.stop("hotrod-rolling-upgrade-3-old-dist");
            }
            if (controller.isStarted("hotrod-rolling-upgrade-4-old-dist")) {
                controller.stop("hotrod-rolling-upgrade-4-old-dist");
            }
        }
    }

    protected RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans cacheBeans) {
        if (System.getProperty("hotrod.protocol.version") != null) {
            // we might want to test backwards compatibility as well
            // old Hot Rod protocol version was set for communication with new server
            return createCache(cacheBeans, System.getProperty("hotrod.protocol.version"));
        } else {
            return createCache(cacheBeans, ConfigurationProperties.DEFAULT_PROTOCOL_VERSION);
        }
    }

    protected RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans cacheBeans, String protocolVersion) {
        return rcmFactory.createCache(cacheBeans, protocolVersion);
    }

    protected RemoteInfinispanMBeans createRemotes(String serverName, String managerName, String cacheName) {
        return RemoteInfinispanMBeans.create(serverManager, serverName, cacheName, managerName);
    }

    private Object invokeOperation(MBeanServerConnectionProvider provider, String mbean, String operationName, Object[] params,
                                   String[] signature) throws Exception {
        return provider.getConnection().invoke(new ObjectName(mbean), operationName, params, signature);
    }
}
