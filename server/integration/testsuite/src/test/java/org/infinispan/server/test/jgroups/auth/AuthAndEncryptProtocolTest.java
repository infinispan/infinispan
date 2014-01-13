package org.infinispan.server.test.jgroups.auth;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.util.TestUtil.getAttribute;
import static org.junit.Assert.assertEquals;

/**
 * Tests for JGroups AUTH and ENCRYPT protocols. Nodes with correct certificate should be allowed to join the cluster, others
 * should not. Communication within cluster should be encrypted.
 *
 * TODO: Check replay attack
 *
 * Command used to generate the certificate for ENCRYPT protocol:
 * keytool -genseckey -alias memcached -keypass secret -storepass secret -keyalg DESede -keysize 168 -keystore server_jceks.keystore -storetype  JCEKS
 * Command used to inspect the certificate:
 * keytool -list -v -keystore server_jceks.keystore  -storetype JCEKS
 *
 * Certificate for AUTH protocol re-used from tests for HotRod SSL (RSA, 2048 bits)
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
public class AuthAndEncryptProtocolTest {

    @InfinispanResource
    RemoteInfinispanServers servers;

    @ArquillianResource
    ContainerController controller;

    final String COORDINATOR_NODE = "clustered-auth-1";
    final String JOINING_NODE_FRIEND = "clustered-auth-2";
    final String COORDINATOR_NODE_NO_ENCRYPT = "clustered-auth-3";
    final String JOINING_NODE_ALIEN = "clustered-auth-4"; //having different certificate

    final String ENCRYPT_MBEAN = "jgroups:type=protocol,cluster=\"clustered\",protocol=ENCRYPT";
    final String AUTH_MBEAN = "jgroups:type=protocol,cluster=\"clustered\",protocol=AUTH";

    @BeforeClass
    public static void before() {
        // ibm7 and ibm6 can't read the keystores created by oracle java, so we need to use the one created on ibm7
        if (System.getProperty("java.vendor").toLowerCase().contains("ibm") &&
              (System.getProperty("java.version").contains("1.7") || System.getProperty("java.version").contains("1.6"))) {
            replaceKeyStoreInConfig(System.getProperty("server1.dist"));
            replaceKeyStoreInConfig(System.getProperty("server2.dist"));
        }
    }

    private static void replaceKeyStoreInConfig(String serverDir) {
        try {
            File configFile = new File(serverDir + "/standalone/configuration/testsuite/clustered-auth-with-encrypt.xml");
            String configContent = FileUtils.readFileToString(configFile, "UTF-8");
            configContent = configContent.replaceAll("server_jceks.keystore", "ibm7_server_jceks.keystore");
            FileUtils.writeStringToFile(configFile, configContent, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException("Replacing the keystore in configuration failed ", e);
        }
    }

    @WithRunningServer(COORDINATOR_NODE)
    @Test
    public void testFriendlyNodeCanJoin() throws Exception {
        try {
            controller.start(JOINING_NODE_FRIEND);
            RemoteInfinispanMBeans coordinator = RemoteInfinispanMBeans.create(servers, COORDINATOR_NODE, "memcachedCache", "clustered");
            RemoteInfinispanMBeans friend = RemoteInfinispanMBeans.create(servers, JOINING_NODE_FRIEND, "memcachedCache", "clustered");
            MBeanServerConnectionProvider providerCoordinator = new MBeanServerConnectionProvider(coordinator.server.getHotrodEndpoint().getInetAddress().getHostName(), 9999);
            MBeanServerConnectionProvider providerFriend = new MBeanServerConnectionProvider(friend.server.getHotrodEndpoint().getInetAddress().getHostName(), 10099);
            MemcachedClient mcCoordinator = new MemcachedClient(coordinator.server.getMemcachedEndpoint().getInetAddress().getHostName(),
                    coordinator.server.getMemcachedEndpoint().getPort());
            MemcachedClient mcFriend = new MemcachedClient(friend.server.getMemcachedEndpoint().getInetAddress().getHostName(),
                    friend.server.getMemcachedEndpoint().getPort());

            //check the cluster was formed
            assertEquals(2, coordinator.manager.getClusterSize());
            assertEquals(2, friend.manager.getClusterSize());

            //check that required protocols (AUTH,ENCRYPT) are registered with JGroups
            assertEquals("secret", getAttribute(providerCoordinator, ENCRYPT_MBEAN, "store_password"));
            assertEquals("secret", getAttribute(providerFriend, ENCRYPT_MBEAN, "store_password"));
            assertEquals("org.jgroups.auth.X509Token", getAttribute(providerCoordinator, AUTH_MBEAN, "auth_class"));
            assertEquals("org.jgroups.auth.X509Token", getAttribute(providerFriend, AUTH_MBEAN, "auth_class"));

            mcFriend.set("key1", "value1");
            assertEquals("Could not read replicated pair key1/value1", "value1", mcCoordinator.get("key1"));
        } finally {
            controller.stop(JOINING_NODE_FRIEND);
        }
    }

    @WithRunningServer(COORDINATOR_NODE_NO_ENCRYPT)
    @Test
    public void testAlienNodeCannotJoin() throws Exception {
        try {
            controller.start(JOINING_NODE_ALIEN);
            //still only one node in cluster expected - no join happened
            assertEquals(1, servers.getServer(COORDINATOR_NODE_NO_ENCRYPT).getCacheManager("clustered").getClusterSize());
        } finally {
            controller.stop(JOINING_NODE_ALIEN);
        }
    }
}