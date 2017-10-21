package org.infinispan.server.test.rollingupgrades;

import javax.management.ObjectName;

import org.apache.http.HttpStatus;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.infinispan.server.test.category.RollingUpgrades;
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
@Category({RollingUpgrades.class})
public class RestRollingUpgradesIT {

    @InfinispanResource
    RemoteInfinispanServers serverManager;

    static final String DEFAULT_CACHE_NAME = "default";
    static final int PORT_OFFSET = 100;

    @ArquillianResource
    ContainerController controller;

    @Test
    public void testRestRollingUpgradesDiffVersions() throws Exception {
        // target node
        final int managementPortServer1 = 9990;
        MBeanServerConnectionProvider provider1;

        RESTHelper rest = new RESTHelper();

        try {

            if (!Boolean.parseBoolean(System.getProperty("start.jboss.as.manually"))) {
                // start it by Arquillian
                controller.start("rest-rolling-upgrade-2-old");
            }

            rest.addServer("127.0.0.1", "/rest");

            rest.post(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "key1", PORT_OFFSET), "data", "text/html");
            rest.get(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "key1", PORT_OFFSET), "data");

            for (int i = 0; i < 50; i++) {
                rest.post(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "keyLoad" + i, PORT_OFFSET), "valueLoad" + i, "text/html");
            }

            controller.start("rest-rolling-upgrade-1");

            RemoteInfinispanMBeans s1 = createRemotes("rest-rolling-upgrade-1", "local", DEFAULT_CACHE_NAME);
            rest.addServer(s1.server.getRESTEndpoint().getInetAddress().getHostName(), s1.server.getRESTEndpoint().getContextPath());

            rest.get(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "key1", 0), "data");

            provider1 = new MBeanServerConnectionProvider(s1.server.getRESTEndpoint().getInetAddress().getHostName(),
                    managementPortServer1);

            final ObjectName rollMan = new ObjectName("jboss." + InfinispanSubsystem.SUBSYSTEM_NAME + ":type=Cache," + "name=\"default(local)\","
                    + "manager=\"local\"," + "component=RollingUpgradeManager");

            invokeOperation(provider1, rollMan.toString(), "synchronizeData", new Object[]{"rest"},
                    new String[]{"java.lang.String"});

            invokeOperation(provider1, rollMan.toString(), "disconnectSource", new Object[]{"rest"},
                    new String[]{"java.lang.String"});

            rest.post(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "disconnected", PORT_OFFSET), "source", "text/plain");

            //Source node entries should NOT be accessible from target node
            rest.get(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "disconnected", 0), HttpStatus.SC_NOT_FOUND);

            //All remaining entries migrated?
            for (int i = 0; i < 50; i++) {
                rest.get(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "keyLoad" + i, 0), "valueLoad" + i);
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

    protected RemoteInfinispanMBeans createRemotes(String serverName, String managerName, String cacheName) {
        return RemoteInfinispanMBeans.create(serverManager, serverName, cacheName, managerName);
    }

    private Object invokeOperation(MBeanServerConnectionProvider provider, String mbean, String operationName, Object[] params,
                                   String[] signature) throws Exception {
        return provider.getConnection().invoke(new ObjectName(mbean), operationName, params, signature);
    }
}
