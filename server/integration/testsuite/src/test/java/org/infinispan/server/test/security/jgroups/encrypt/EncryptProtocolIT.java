package org.infinispan.server.test.security.jgroups.encrypt;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.SERVER2_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.getAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.core.RunningServer;
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

/**
 * Test JGroups' ENCRYPT protocol. Only proper registration of the protocol is tested, making
 * sure that the server can work with ENCRYPT protocol. This test does NOT check whether the
 * communication between nodes is really encrypted.
 *
 * Command used to generate the certificate for ENCRYPT protocol:
 * keytool -genseckey -alias memcached -keypass secret -storepass secret -keyalg DESede -keysize 168 -keystore server_jceks.keystore -storetype  JCEKS
 * Command used to inspect the certificate:
 * keytool -list -v -keystore server_jceks.keystore  -storetype JCEKS
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
public class EncryptProtocolIT {

    @InfinispanResource
    RemoteInfinispanServers servers;

    @ArquillianResource
    ContainerController controller;

    final String COORDINATOR_NODE = "clustered-encrypt-1";
    final String JOINING_NODE = "clustered-encrypt-2";

    final String ENCRYPT_MBEAN = "jgroups:type=protocol,cluster=\"clustered\",protocol=ENCRYPT";
    final String ENCRYPT_PROPERTY_KEY = "key_store_name";
    final String ENCRYPT_PROPERTY_VALUE_SUFFIX = "server_jceks.keystore";
    final String ENCRYPT_PASSWORD_KEY = "store_password";

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
            File configFile = new File(serverDir + "/standalone/configuration/testsuite/clustered-with-encrypt.xml");
            String configContent = FileUtils.readFileToString(configFile, "UTF-8");
            configContent = configContent.replaceAll("server_jceks.keystore", "ibm7_server_jceks.keystore");
            FileUtils.writeStringToFile(configFile, configContent, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException("Replacing the keystore in configuration failed ", e);
        }
    }

    @Test
    @WithRunningServer(@RunningServer(name = COORDINATOR_NODE))
    public void testEncryptProtocolRegistered() throws Exception {
        try {
            controller.start(JOINING_NODE);
            RemoteInfinispanMBeans coordinator = RemoteInfinispanMBeans.create(servers, COORDINATOR_NODE, "memcachedCache", "clustered");
            RemoteInfinispanMBeans friend = RemoteInfinispanMBeans.create(servers, JOINING_NODE, "memcachedCache", "clustered");
            MBeanServerConnectionProvider providerCoordinator = new MBeanServerConnectionProvider(coordinator.server.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);
            MBeanServerConnectionProvider providerFriend = new MBeanServerConnectionProvider(friend.server.getHotrodEndpoint().getInetAddress().getHostName(), SERVER2_MGMT_PORT);
            MemcachedClient mcCoordinator = new MemcachedClient(coordinator.server.getMemcachedEndpoint().getInetAddress().getHostName(),
                    coordinator.server.getMemcachedEndpoint().getPort());
            MemcachedClient mcFriend = new MemcachedClient(friend.server.getMemcachedEndpoint().getInetAddress().getHostName(),
                    friend.server.getMemcachedEndpoint().getPort());

            //check the cluster was formed
            assertEquals(2, coordinator.manager.getClusterSize());
            assertEquals(2, friend.manager.getClusterSize());

            //check that ENCRYPT protocol is registered with JGroups
            assertTrue(getAttribute(providerCoordinator, ENCRYPT_MBEAN, ENCRYPT_PROPERTY_KEY).endsWith(ENCRYPT_PROPERTY_VALUE_SUFFIX));
            assertTrue(getAttribute(providerFriend, ENCRYPT_MBEAN, ENCRYPT_PROPERTY_KEY).endsWith(ENCRYPT_PROPERTY_VALUE_SUFFIX));
            
            //JGRP-1854: check that ENCRYPT password is not visible via JMX 
            assertNull(getAttribute(providerCoordinator, ENCRYPT_MBEAN, ENCRYPT_PASSWORD_KEY));

            mcFriend.set("key1", "value1");
            assertEquals("Could not read replicated pair key1/value1", "value1", mcCoordinator.get("key1"));
        } finally {
            controller.stop(JOINING_NODE);
        }
    }
}
