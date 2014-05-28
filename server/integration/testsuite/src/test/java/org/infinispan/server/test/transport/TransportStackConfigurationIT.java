package org.infinispan.server.test.transport;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;

import java.util.Scanner;

import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.jboss.arquillian.container.test.api.Config;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.util.ITestUtils.getAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the stack attribute of the transport element. The
 * stack attribute is set to UDP.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@RunWith(Arquillian.class)
public class TransportStackConfigurationIT {

    private final String CONTAINER1 = "transport-stack-1";
    private final String CONTAINER2 = "transport-stack-2";

    @InfinispanResource(CONTAINER1)
    RemoteInfinispanServer server1;

    @ArquillianResource
    ContainerController controller;
    private MBeanServerConnectionProvider provider;
    String udpProtocolMBean = "jgroups:type=protocol,cluster=\"clustered\",protocol=UDP";
    String tcpProtocolMBean = "jgroups:type=protocol,cluster=\"clustered\",protocol=TCP";

    /*
     * When setting a stack attribute on <transport> config. element, there has to be a UDP protocol 
     * MBean available via JMX. Its attributes must match those in standalone.xml config file (in fact,
     * these are default values taken from jgroups-defaults.xml configuration file in clustering/jgroups
     * subsystem)
     */
    @Test
    public void testUDPStackAttributes() throws Exception {
        try {
            startContainerWithStack(CONTAINER1, "node0", 0,   "udp");
            startContainerWithStack(CONTAINER2, "node1", 100, "udp");

            provider = new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);

            assertMBeanAttributes(provider, udpProtocolMBean);
        } finally {
            stopContainers(CONTAINER1, CONTAINER2);
        }
    }

    /*
     * When setting a stack attribute on <transport> config. element, there has to be a TCP protocol
     * MBean available via JMX. Its attributes must match those in standalone.xml config file (in fact,
     * these are default values taken from jgroups-defaults.xml configuration file in clustering/jgroups
     * subsystem).
     */
    @Test
    public void testTCPStackAttributes() throws Exception {
        try {
            startContainerWithStack(CONTAINER1, "node0", 0,   "tcp");
            startContainerWithStack(CONTAINER2, "node1", 100, "tcp");

            provider = new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);


            assertMBeanAttributes(provider, tcpProtocolMBean);
        } finally {
            stopContainers(CONTAINER1, CONTAINER2);
        }
    }

    private void assertMBeanAttributes(MBeanServerConnectionProvider provider, String protocolMBean) throws Exception {
        assertEquals(true, Boolean.parseBoolean(getAttribute(provider, protocolMBean, "discard_incompatible_packets")));
        assertEquals(true, Boolean.parseBoolean(getAttribute(provider, protocolMBean, "enable_bundling")));

        assertEquals(true, Boolean.parseBoolean(getAttribute(provider, protocolMBean, "thread_pool.enabled")));
        assertEquals(true, Boolean.parseBoolean(getAttribute(provider, protocolMBean, "thread_pool.queue_enabled")));
        assertEquals("discard", getAttribute(provider, protocolMBean, "thread_pool.rejection_policy"));

        assertEquals(true, Boolean.parseBoolean(getAttribute(provider, protocolMBean, "oob_thread_pool.enabled")));
        assertEquals(true, Boolean.parseBoolean(getAttribute(provider, protocolMBean, "oob_thread_pool.queue_enabled")));
        assertEquals("discard", getAttribute(provider, protocolMBean, "oob_thread_pool.rejection_policy"));
    }

    /*
     * Dump service via JMX and find out whether test-infinispan-transport executor was picked up.
     */
    @Test
    public void testExecutorAttribute() throws Exception {
        try {
            startContainerWithStack(CONTAINER1, "node0", 0,   "tcp");
            startContainerWithStack(CONTAINER2, "node1", 100, "tcp");

            provider = new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);
            final String dumpServicesBean = "jboss.msc:type=container,name=jboss-as";
            final String dumpServicesOp = "dumpServicesToString";
            String services = provider.getConnection().invoke(new ObjectName(dumpServicesBean), dumpServicesOp, null, null).toString();
            assertTrue(isTestInfinispanTransportSpecified(services));
        } finally {
            stopContainers(CONTAINER1, CONTAINER2);
        }
    }

    private boolean isTestInfinispanTransportSpecified(String services) {
        Scanner s = new Scanner(services).useDelimiter("\n");
        while (true) {
            try {
                String line = s.nextLine();
                if (line.contains("Service \"jboss.infinispan.clustered.config\"") &&
                        line.substring(line.indexOf("dependencies:")).contains("jboss.thread.executor.infinispan-transport")) {
                    return true;
                }
            } catch (Exception e) {
                return false;
            }
        }
    }

    private void startContainerWithStack(String containerName, String nodeName, int portOffset, String stack) {

        controller.start(containerName, new Config().add("javaVmArguments", System.getProperty("server.jvm.args")
                + " -Djboss.node.name=" + nodeName
                + " -Djboss.socket.binding.port-offset=" + portOffset
                + " -Djboss.default.jgroups.stack=" + stack
        ).map());

    }

    private void stopContainers(String... containerNames) {
        for (String name : containerNames) {
            controller.stop(name);
        }
    }
}
