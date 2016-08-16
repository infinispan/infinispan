package org.infinispan.test.integration.security.embedded;

import org.infinispan.test.integration.security.utils.Deployments;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.as.arquillian.api.ServerSetup;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author <a href="mailto:vjuranek@redhat.com">Vojtech Juranek</a>
 * @since 7.0
 */
@RunWith(Arquillian.class)
@ServerSetup({
      NodeAuthenticationKrbFailIT.KrbLdapServerSetupTask.class,
      NodeAuthenticationKrbFailIT.Krb5ConfServerSetupTask.class
})
public class NodeAuthenticationKrbFailIT extends AbstractNodeAuthentication {

   protected static final String JOINING_NODE = "node1";

   public NodeAuthenticationKrbFailIT() {
      super(true);
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
      return JOINING_NODE_JGROUSP_CONFIG_KRB_FAIL;
   }

   @Deployment(name = COORDINATOR_NODE, managed = false)
   @TargetsContainer(COORDINATOR_NODE)
   public static WebArchive getCoordinatorDeployment() {
      return Deployments.createNodeAuthKrbTestDeployment(COORDINATOR_JGROUSP_CONFIG_KRB);
   }

   @Deployment(name = JOINING_NODE, managed = false)
   @TargetsContainer(JOINING_NODE)
   public static WebArchive getJoiningNodeDeployment() {
      return Deployments.createNodeAuthKrbTestDeployment(JOINING_NODE_JGROUSP_CONFIG_KRB_FAIL);
   }

   @Override
   @Test(expected = org.infinispan.manager.EmbeddedCacheManagerStartupException.class)
   @OperateOnDeployment(JOINING_NODE)
   @InSequence(3)
   public void testReadItemOnJoiningNode() throws Exception {
      super.testReadItemOnJoiningNode();
   }
}
