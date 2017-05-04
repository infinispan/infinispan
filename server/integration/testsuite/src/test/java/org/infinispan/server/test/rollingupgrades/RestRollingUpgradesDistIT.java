package org.infinispan.server.test.rollingupgrades;

import javax.management.ObjectName;

import org.apache.http.HttpStatus;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.infinispan.server.test.category.RollingUpgradesDist;
import org.infinispan.server.test.client.rest.RESTHelper;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for REST rolling upgrades.
 *
 * @author Tomas Sykora (tsykora@redhat.com)
 * @author Martin Gencur (mgencur@redhat.com)
 */
@Ignore("Disabled until ISPN-7617 is fixed")
@RunWith(Arquillian.class)
@Category({RollingUpgradesDist.class})
public class RestRollingUpgradesDistIT {

    @InfinispanResource
    RemoteInfinispanServers serverManager;

    static final String DEFAULT_CACHE_NAME = "default";
    static final int PORT_OFFSET = 100;
    static final int PORT_OFFSET_200 = 200;
    static final int PORT_OFFSET_300 = 300;

    @ArquillianResource
    ContainerController controller;

    @Test
    public void testRestRollingUpgradesDiffVersionsDist() throws Exception {
        // Target node
        final int managementPortServer1 = 9990;
        MBeanServerConnectionProvider provider1;
        final int managementPortServer2 = 10090;
        // Source node
        int managementPortServer3 = 10199;
        MBeanServerConnectionProvider provider3;

        RESTHelper rest = new RESTHelper();

        if (!Boolean.parseBoolean(System.getProperty("start.jboss.as.manually"))) {
            // start it by Arquillian
            controller.start("rest-rolling-upgrade-3-old-dist");
            controller.start("rest-rolling-upgrade-4-old-dist");
            managementPortServer3 = 10190;
        }

        try {
            // port offset 200, server3, index 0 in RESTHelper
            rest.addServer("127.0.0.1", "/rest");

            // port offset 300, server4, index 1 in RESTHelper
            rest.addServer("127.0.0.1", "/rest");

            rest.post(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "key1", PORT_OFFSET_200), "data", "text/html");
            rest.get(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "key1", PORT_OFFSET_200), "data");
            rest.post(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "key1x", PORT_OFFSET_300), "datax", "text/html");
            rest.get(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "key1x", PORT_OFFSET_300), "datax");

            for (int i = 0; i < 50; i++) {
                rest.post(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "keyLoad" + i, PORT_OFFSET_200), "valueLoad" + i, "text/html");
                rest.post(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "keyLoadx" + i, PORT_OFFSET_300), "valueLoadx" + i, "text/html");
            }

            controller.start("rest-rolling-upgrade-1-dist");
            controller.start("rest-rolling-upgrade-2-dist");

            // port offset 0, server0, index 2 in RESTHelper
            RemoteInfinispanMBeans s1 = createRemotes("rest-rolling-upgrade-1-dist", "clustered-new", DEFAULT_CACHE_NAME);
            rest.addServer(s1.server.getRESTEndpoint().getInetAddress().getHostName(), s1.server.getRESTEndpoint().getContextPath());

            // port offset 100, server1, index 3 in RESTHelper
            RemoteInfinispanMBeans s2 = createRemotes("rest-rolling-upgrade-2-dist", "clustered-new", DEFAULT_CACHE_NAME);
            rest.addServer(s2.server.getRESTEndpoint().getInetAddress().getHostName(), s2.server.getRESTEndpoint().getContextPath());

            // test cross-fetching of entries from stores
            // if fails, it probably can't access entries stored in source node (target's RemoteCacheStore).
            rest.get(rest.fullPathKey(2, DEFAULT_CACHE_NAME, "key1", 0), "data");
            rest.get(rest.fullPathKey(2, DEFAULT_CACHE_NAME, "key1x", 0), "datax");
            rest.get(rest.fullPathKey(3, DEFAULT_CACHE_NAME, "key1", PORT_OFFSET), "data");
            rest.get(rest.fullPathKey(3, DEFAULT_CACHE_NAME, "key1x", PORT_OFFSET), "datax");

            provider1 = new MBeanServerConnectionProvider(s1.server.getRESTEndpoint().getInetAddress().getHostName(),
                    managementPortServer1);

            provider3 = new MBeanServerConnectionProvider("127.0.0.1", managementPortServer3);

            final ObjectName rollMan3 = new ObjectName("jboss." + InfinispanSubsystem.SUBSYSTEM_NAME + ":type=Cache," + "name=\"default(dist_sync)\","
                    + "manager=\"clustered\"," + "component=RollingUpgradeManager");

            invokeOperation(provider3, rollMan3.toString(), "recordKnownGlobalKeyset", new Object[]{}, new String[]{});

            final ObjectName rollMan1 = new ObjectName("jboss." + InfinispanSubsystem.SUBSYSTEM_NAME + ":type=Cache," + "name=\"default(dist_sync)\","
                    + "manager=\"clustered-new\"," + "component=RollingUpgradeManager");

            invokeOperation(provider1, rollMan1.toString(), "synchronizeData", new Object[]{"rest"},
                    new String[]{"java.lang.String"});

            invokeOperation(provider1, rollMan1.toString(), "disconnectSource", new Object[]{"rest"},
                    new String[]{"java.lang.String"});

            invokeOperation(new MBeanServerConnectionProvider(s2.server.getRESTEndpoint().getInetAddress().getHostName(),
                            managementPortServer2), rollMan1.toString(), "disconnectSource", new Object[]{"rest"},
                    new String[]{"java.lang.String"});

            // 2 puts into source cluster
            rest.post(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "disconnected", PORT_OFFSET_200), "source", "text/html");
            rest.post(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "disconnectedx", PORT_OFFSET_300), "sourcex", "text/html");

            rest.get(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "disconnected", PORT_OFFSET_200), "source");
            rest.get(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "disconnectedx", PORT_OFFSET_200), "sourcex");
            rest.get(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "disconnected", PORT_OFFSET_300), "source");
            rest.get(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "disconnectedx", PORT_OFFSET_300), "sourcex");

            // is RemoteCacheStore really disconnected?
            // source node entries should NOT be accessible from target node now
            rest.get(rest.fullPathKey(2, DEFAULT_CACHE_NAME, "disconnected", 0), HttpStatus.SC_NOT_FOUND);
            rest.get(rest.fullPathKey(3, DEFAULT_CACHE_NAME, "disconnected", PORT_OFFSET), HttpStatus.SC_NOT_FOUND);
            rest.get(rest.fullPathKey(2, DEFAULT_CACHE_NAME, "disconnectedx", 0), HttpStatus.SC_NOT_FOUND);
            rest.get(rest.fullPathKey(3, DEFAULT_CACHE_NAME, "disconnectedx", PORT_OFFSET), HttpStatus.SC_NOT_FOUND);

            // all entries migrated?
            rest.get(rest.fullPathKey(2, DEFAULT_CACHE_NAME, "key1", 0), "data");
            for (int i = 0; i < 50; i++) {
                rest.get(rest.fullPathKey(2, DEFAULT_CACHE_NAME, "keyLoad" + i, 0), "valueLoad" + i);
                // clustered => all entries should be migrated and accessible
                rest.get(rest.fullPathKey(2, DEFAULT_CACHE_NAME, "keyLoadx" + i, 0), "valueLoadx" + i);
            }
        } finally {
            if (controller.isStarted("rest-rolling-upgrade-1-dist")) {
                controller.stop("rest-rolling-upgrade-1-dist");
            }
            if (controller.isStarted("rest-rolling-upgrade-2-dist")) {
                controller.stop("rest-rolling-upgrade-2-dist");
            }
            if (controller.isStarted("rest-rolling-upgrade-3-old-dist")) {
                controller.stop("rest-rolling-upgrade-3-old-dist");
            }
            if (controller.isStarted("rest-rolling-upgrade-4-old-dist")) {
                controller.stop("rest-rolling-upgrade-4-old-dist");
            }
        }
    }

    protected RemoteInfinispanMBeans createRemotes(String serverName, String managerName, String cacheName) {
        return RemoteInfinispanMBeans.create(serverManager, serverName, cacheName, managerName);
    }

    private Object invokeOperation(MBeanServerConnectionProvider provider, String mbean, String operationName, Object[] params,
                                   String[] signature) throws Exception {
        return provider.getConnection().invoke(new ObjectName(mbean), operationName, params, signature);
    }
}
