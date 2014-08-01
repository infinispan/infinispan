package org.infinispan.test.integration.security.embedded;

import org.infinispan.test.integration.security.utils.ApacheDsKrbLdap;
import org.infinispan.test.integration.security.utils.Deployments;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *  
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class NodeAuthenticationKrbPassIT extends AbstractNodeAuthentication {

   protected static final String JOINING_NODE = "node1";
   
   private static ApacheDsKrbLdap krbLdapServer;
   
   @BeforeClass
   public static void kerberosSetup() throws Exception {
      krbLdapServer = new ApacheDsKrbLdap("localhost");
      krbLdapServer.start();
   }

   @AfterClass
   public static void ldapTearDown() throws Exception {
      krbLdapServer.stop();
   }

   @Override
   protected String getCoordinatorNodeConfig() {
      return COORDINATOR_JGROUSP_CONFIG_KRB;
   }

   @Override
   protected String getJoiningNodeName() {
      return JOINING_NODE;
   }

   @Override
   protected String getJoiningNodeConfig() {
      return JOINING_NODE_JGROUSP_CONFIG_KRB;
   }

   @Deployment(name = COORDINATOR_NODE, managed = false)
   @TargetsContainer(COORDINATOR_NODE)
   public static WebArchive getCoordinatorDeployment() {
      return Deployments.createNodeAuthKrbTestDeployment(COORDINATOR_JGROUSP_CONFIG_KRB);
   }

   @Deployment(name = JOINING_NODE, managed = false)
   @TargetsContainer(JOINING_NODE)
   public static WebArchive getJoiningNodeDeployment() {
      return Deployments.createNodeAuthKrbTestDeployment(JOINING_NODE_JGROUSP_CONFIG_KRB);
   }

   @Override
   @Test
   @OperateOnDeployment(JOINING_NODE)
   @InSequence(3)
   public void testReadItemOnJoiningNode() throws Exception {
      super.testReadItemOnJoiningNode();
   }

}
