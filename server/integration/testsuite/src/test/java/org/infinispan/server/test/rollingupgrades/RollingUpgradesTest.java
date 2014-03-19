package org.infinispan.server.test.rollingupgrades;

import javax.management.ObjectName;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.server.test.category.RollingUpgrades;
import org.infinispan.server.test.client.rest.RESTHelper;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.client.rest.RESTHelper.KEY_A;
import static org.infinispan.server.test.client.rest.RESTHelper.fullPathKey;
import static org.infinispan.server.test.client.rest.RESTHelper.get;
import static org.infinispan.server.test.client.rest.RESTHelper.head;
import static org.infinispan.server.test.client.rest.RESTHelper.put;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests for rolling upgrades functionality.
 *
 * @author Tomas Sykora (tsykora@redhat.com)
 */
@RunWith(Arquillian.class)
@Category({RollingUpgrades.class})
public class RollingUpgradesTest {

    private static final Logger log = Logger.getLogger(RollingUpgradesTest.class);

    @InfinispanResource
    RemoteInfinispanServers serverManager;

    static final String DEFAULT_CACHE_NAME = "default";

    @InfinispanResource("rest-rolling-upgrade-1")
    RemoteInfinispanServer server1;

    @InfinispanResource("rest-rolling-upgrade-2-old")
    RemoteInfinispanServer server2;

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
        final int managementPortServer1 = 9999;
        MBeanServerConnectionProvider provider1;
        // Source node
        final int managementPortServer2 = 10099;
        MBeanServerConnectionProvider provider2;

        controller.start("hotrod-rolling-upgrade-2-old");
        try {
            RemoteInfinispanMBeans s2 = createRemotes("hotrod-rolling-upgrade-2-old", "local", DEFAULT_CACHE_NAME);
            final RemoteCache<Object, Object> c2 = createCache(s2, ConfigurationProperties.PROTOCOL_VERSION_12);

            c2.put("key1", "value1");
            assertEquals("value1", c2.get("key1"));

            for (int i = 0; i < 50; i++) {
                c2.put("keyLoad" + i, "valueLoad" + i);
            }

            controller.start("hotrod-rolling-upgrade-1");

            RemoteInfinispanMBeans s1 = createRemotes("hotrod-rolling-upgrade-1", "local", DEFAULT_CACHE_NAME);
            final RemoteCache<Object, Object> c1 = createCache(s1);

            assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "value1", c1.get("key1"));

            provider1 = new MBeanServerConnectionProvider(s1.server.getHotrodEndpoint().getInetAddress().getHostName(),
                    managementPortServer1);
            provider2 = new MBeanServerConnectionProvider(s2.server.getHotrodEndpoint().getInetAddress().getHostName(),
                    managementPortServer2);

            final ObjectName rollMan = new ObjectName("jboss.infinispan:type=Cache," + "name=\"default(local)\","
                    + "manager=\"local\"," + "component=RollingUpgradeManager");

            invokeOperation(provider2, rollMan.toString(), "recordKnownGlobalKeyset", new Object[]{}, new String[]{});

            invokeOperation(provider1, rollMan.toString(), "synchronizeData", new Object[]{"hotrod"},
                    new String[]{"java.lang.String"});

            invokeOperation(provider1, rollMan.toString(), "disconnectSource", new Object[]{"hotrod"},
                    new String[]{"java.lang.String"});

            // is source (RemoteCacheStore) really disconnected?
            c2.put("disconnected", "source");
            assertEquals("Can't obtain value from cache1 (source node).", "source", c2.get("disconnected"));
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


    @Test
    public void testRestRollingUpgrades() throws Exception {
        // Target node
        final int managementPortServer1 = 9999;
        MBeanServerConnectionProvider provider1;
        // Source node
        final int managementPortServer2 = 10099;
        MBeanServerConnectionProvider provider2;
        try {

            System.out.println("\nStarting source node -- node2 \n");

            controller.start("rest-rolling-upgrade-2-old");
            RESTHelper.addServer(server2.getRESTEndpoint().getInetAddress().getHostName(), server2.getRESTEndpoint().getContextPath());

            for (int i = 0; i < 1000; i++) {
                put(fullPathKey(0, "default", "rkey" + i, 100), "rval" + i, "text/plain");
            }

            controller.start("rest-rolling-upgrade-1");
            RESTHelper.addServer(server1.getRESTEndpoint().getInetAddress().getHostName(), server1.getRESTEndpoint().getContextPath());


            provider1 = new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), managementPortServer1);
            provider2 = new MBeanServerConnectionProvider(server2.getHotrodEndpoint().getInetAddress().getHostName(), managementPortServer2);

            final ObjectName rollMan = new ObjectName("jboss.infinispan:type=Cache," + "name=\"default(local)\","
                    + "manager=\"local\"," + "component=RollingUpgradeManager");

            invokeOperation(provider2, rollMan.toString(), "recordKnownGlobalKeyset", new Object[]{}, new String[]{});

            invokeOperation(provider1, rollMan.toString(), "synchronizeData", new Object[]{"rest"},
                    new String[]{"java.lang.String"});

            invokeOperation(provider1, rollMan.toString(), "disconnectSource", new Object[]{"rest"},
                    new String[]{"java.lang.String"});

            // is source (RemoteCacheStore) really disconnected?
            put(fullPathKey(0, "default", "disconnected", 100), "source", "text/plain");
//            head(fullPathKey(0, "default", "disconnected", 100), HttpServletResponse.SC_NOT_FOUND);
//            head(fullPathKey(1, "default", "disconnected", 0), HttpServletResponse.SC_NOT_FOUND);

            // all entries migrated?
            for (int i = 0; i < 1000; i++) {
                get(fullPathKey(1, "default", "rkey" + i, 0), "rval" + i);
            }
        } finally {
            if (controller.isStarted("rest-rolling-upgrade-1")) {
                controller.stop("rest-rolling-upgrade-1");
            }
            if (controller.isStarted("rest-rolling-upgrade-2-old")) {
                controller.stop("rest-rolling-upgrade-2-old");
            }
        }
    }


    protected RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans cacheBeans) {
        return createCache(cacheBeans, ConfigurationProperties.DEFAULT_PROTOCOL_VERSION);
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
