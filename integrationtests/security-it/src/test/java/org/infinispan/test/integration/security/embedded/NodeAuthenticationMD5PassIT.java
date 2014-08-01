package org.infinispan.test.integration.security.embedded;

import org.infinispan.test.integration.security.utils.Deployments;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * 
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class NodeAuthenticationMD5PassIT extends AbstractNodeAuthentication {

   protected static final String JOINING_NODE = "node1";

   @Override
   protected String getCoordinatorNodeConfig() {
      return COORDINATOR_JGROUSP_CONFIG_MD5;
   }

   @Override
   protected String getJoiningNodeName() {
      return JOINING_NODE;
   }

   @Override
   protected String getJoiningNodeConfig() {
      return JOINING_NODE_JGROUSP_CONFIG_MD5;
   }

   @Deployment(name = COORDINATOR_NODE, managed = false)
   @TargetsContainer(COORDINATOR_NODE)
   public static WebArchive getCoordinatorDeployment() {
      return Deployments.createNodeAuthTestDeployment(COORDINATOR_JGROUSP_CONFIG_MD5);
   }
   
   @Deployment(name = JOINING_NODE, managed = false)
   @TargetsContainer(JOINING_NODE)
   public static WebArchive getJoiningNodeDeployment() {
      return Deployments.createNodeAuthTestDeployment(JOINING_NODE_JGROUSP_CONFIG_MD5);
   }

   @Override
   @Test
   @OperateOnDeployment(JOINING_NODE)
   @InSequence(3)
   public void testReadItemOnJoiningNode() throws Exception {
      super.testReadItemOnJoiningNode();
   }

}
