package org.infinispan.server.test.security.jgroups.sasl;

import static org.infinispan.server.test.util.ITestUtils.getAttribute;
import static org.junit.Assert.assertEquals;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.infinispan.test.integration.security.utils.ApacheDsKrbLdap;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 *
 * Test JGroups' SASL protocol with various mechs (namely with DIGEST-MD5 and GSSAPI).
 *
 * @author Martin Gencur
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
@Category({ Security.class })
public class SaslAuthIT {

   @InfinispanResource
   RemoteInfinispanServers servers;

   @ArquillianResource
   ContainerController controller;

   final String COORDINATOR_NODE_MD5 = "clustered-sasl-md5-1";
   final String JOINING_NODE_MD5 = "clustered-sasl-md5-2";
   final String ANOTHER_JOINING_NODE_MD5 = "another-clustered-sasl-md5-2";
   final String MECH_MD5 = "DIGEST-MD5";

   final String COORDINATOR_NODE_KRB = "clustered-sasl-krb-1";
   final String JOINING_NODE_KRB = "clustered-sasl-krb-2";
   final String MECH_KRB = "GSSAPI";

   final String SASL_MBEAN = "jgroups:type=protocol,cluster=\"cluster\",protocol=SASL";

   private static ApacheDsKrbLdap krbLdapServer;

   @BeforeClass
   public static void ldapSetup() throws Exception {
      krbLdapServer = new ApacheDsKrbLdap("localhost");
      krbLdapServer.start();
   }

   @AfterClass
   public static void ldapTearDown() throws Exception {
      krbLdapServer.stop();
   }

   @Test
   @WithRunningServer(@RunningServer(name = COORDINATOR_NODE_MD5))
   public void testSaslMD5() throws Exception {
      saslTest(COORDINATOR_NODE_MD5, JOINING_NODE_MD5, MECH_MD5);
   }

   @Ignore
   @WithRunningServer(@RunningServer(name = COORDINATOR_NODE_KRB))
   public void testSaslKrb() throws Exception {
      saslTest(COORDINATOR_NODE_KRB, JOINING_NODE_KRB, MECH_KRB);
   }

   @Test
   @WithRunningServer(@RunningServer(name = COORDINATOR_NODE_MD5))
   public void testNodeAuthorization() throws Exception {
      authorizationTest(COORDINATOR_NODE_MD5, ANOTHER_JOINING_NODE_MD5, MECH_MD5);
   }

   public void saslTest(String coordinatorNode, String joiningNode, String mech) throws Exception {
      try {
         controller.start(joiningNode);
         RemoteInfinispanMBeans coordinator = RemoteInfinispanMBeans.create(servers, coordinatorNode, "memcachedCache",
               "clustered");
         RemoteInfinispanMBeans friend = RemoteInfinispanMBeans.create(servers, joiningNode, "memcachedCache",
               "clustered");

         MBeanServerConnectionProvider providerCoordinator = new MBeanServerConnectionProvider(coordinator.server
               .getHotrodEndpoint().getInetAddress().getHostName(), ITestUtils.SERVER1_MGMT_PORT);
         MBeanServerConnectionProvider providerFriend = new MBeanServerConnectionProvider(friend.server
               .getHotrodEndpoint().getInetAddress().getHostName(), ITestUtils.SERVER2_MGMT_PORT);
         MemcachedClient mcCoordinator = new MemcachedClient(coordinator.server.getMemcachedEndpoint().getInetAddress()
               .getHostName(), coordinator.server.getMemcachedEndpoint().getPort());
         MemcachedClient mcFriend = new MemcachedClient(friend.server.getMemcachedEndpoint().getInetAddress()
               .getHostName(), friend.server.getMemcachedEndpoint().getPort());

         //check the cluster was formed
         assertEquals(2, coordinator.manager.getClusterSize());
         assertEquals(2, friend.manager.getClusterSize());

         //check that SASL protocol is registered with JGroups
         assertEquals(mech, getAttribute(providerCoordinator, SASL_MBEAN, "mech"));
         assertEquals(mech, getAttribute(providerFriend, SASL_MBEAN, "mech"));

         mcFriend.set("key1", "value1");
         assertEquals("Could not read replicated pair key1/value1", "value1", mcCoordinator.get("key1"));
      } finally {
         controller.stop(joiningNode);
      }
   }

   public void authorizationTest(String coordinatorNode, String joiningNode, String mech) throws Exception {
      try {
         controller.start(joiningNode);
         RemoteInfinispanMBeans coordinator = RemoteInfinispanMBeans.create(servers, coordinatorNode, "memcachedCache",
               "clustered");
         //check the cluster was NOT formed
         assertEquals(1, coordinator.manager.getClusterSize());
      } finally {
         controller.stop(joiningNode);
      }
   }

}
