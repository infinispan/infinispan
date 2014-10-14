package org.infinispan.server.test.rollingupgrades;

import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.server.test.category.RollingUpgrades;
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
 * Tests for rolling upgrades functionality.
 *
 * @author Tomas Sykora (tsykora@redhat.com)
 */
@RunWith(Arquillian.class)
@Category({RollingUpgrades.class})
public class HotRodRollingUpgradesIT {

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
    public void testHotRodRollingUpgradesDiffVersions() throws Exception {
        // Target node
        final int managementPortServer1 = 9990;
        MBeanServerConnectionProvider provider1;
        // Source node
        int managementPortServer2 = 10099; //jboss-as mgmt port
        MBeanServerConnectionProvider provider2;

        try {

            if (!Boolean.parseBoolean(System.getProperty("start.jboss.as.manually"))) {
                // start it by Arquillian
                controller.start("hotrod-rolling-upgrade-2-old");
                managementPortServer2 = 10090;
            }

            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.addServer()
                    .host("127.0.0.1")
                    .port(11322)
                    .protocolVersion(ConfigurationProperties.PROTOCOL_VERSION_12);

            RemoteCacheManager rcm = new RemoteCacheManager(builder.build());
            final RemoteCache<String, String> c2 = rcm.getCache("default");

            c2.put("key1", "value1");
            assertEquals("value1", c2.get("key1"));

            for (int i = 0; i < 50; i++) {
                c2.put("keyLoad" + i, "valueLoad" + i);
            }

            controller.start("hotrod-rolling-upgrade-1");

            RemoteInfinispanMBeans s1 = createRemotes("hotrod-rolling-upgrade-1", "local", DEFAULT_CACHE_NAME);
            // hotrod.protocol.version, if explictily defined, is set in createRemotes() method
            final RemoteCache<Object, Object> c1 = createCache(s1);

            assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "value1", c1.get("key1"));

            provider2 = new MBeanServerConnectionProvider("127.0.0.1", managementPortServer2);

            final ObjectName rollMan = new ObjectName("jboss.infinispan:type=Cache," + "name=\"default(local)\","
                    + "manager=\"local\"," + "component=RollingUpgradeManager");

            invokeOperation(provider2, rollMan.toString(), "recordKnownGlobalKeyset", new Object[]{}, new String[]{});

            provider1 = new MBeanServerConnectionProvider(s1.server.getHotrodEndpoint().getInetAddress().getHostName(),
                    managementPortServer1);

            invokeOperation(provider1, rollMan.toString(), "synchronizeData", new Object[]{"hotrod"},
                    new String[]{"java.lang.String"});

            invokeOperation(provider1, rollMan.toString(), "disconnectSource", new Object[]{"hotrod"},
                    new String[]{"java.lang.String"});

            // is source (RemoteCacheStore) really disconnected?
            c2.put("disconnected", "source");
            assertEquals("Can't obtain value from cache2 (source node).", "source", c2.get("disconnected"));
            assertNull("Source node entries should NOT be accessible from target node (after RCS disconnection)",
                    c1.get("disconnected"));

            // all entries migrated?
            assertEquals("Entry was not successfully migrated.", "value1", c1.get("key1"));
            for (int i = 0; i < 50; i++) {
                assertEquals("Entry was not successfully migrated.", "valueLoad" + i, c1.get("keyLoad" + i));
            }
        } finally {
            if (controller.isStarted("hotrod-rolling-upgrade-1")) {
                controller.stop("hotrod-rolling-upgrade-1");
            }
            if (controller.isStarted("hotrod-rolling-upgrade-2-old")) {
                controller.stop("hotrod-rolling-upgrade-2-old");
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
