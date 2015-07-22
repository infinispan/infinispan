
 package org.infinispan.test.integration.security.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.integration.security.utils.Deployments;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
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
 * Tests properties based auth callback handler, which is build-in in JGroups.
 * Handler checks provided auth information against configured properties file.
 * For test is used SASL MD5 authentication.
 * 
 * @author vjuranek
 * @since 8.0
 */
@RunWith(Arquillian.class)
public class NodeAuthPropertiesHandlerIT extends AbstractNodeAuthentication {

   protected static final String COORDINATOR_JGROUSP_CONFIG_MD5 = "jgroups-tcp-sasl-prop-handler-node0.xml";
   protected static final String JOINING_NODE_JGROUSP_CONFIG_MD5 = "jgroups-tcp-sasl-prop-handler-node1.xml";
   protected static final String COORDINATOR_NODE = "simple-auth-node0";
   protected static final String JOINING_NODE = "simple-auth-node1";
   
   private static final Log LOG = LogFactory.getLog(NodeAuthPropertiesHandlerIT.class);
   
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

   
   @Test
   @InSequence(1)
   public void startNodes() throws Exception {
      controller.start(COORDINATOR_NODE);
      assertTrue(controller.isStarted(COORDINATOR_NODE));
      controller.start(getJoiningNodeName());
      assertTrue(controller.isStarted(getJoiningNodeName()));
      deployer.deploy(COORDINATOR_NODE);
      deployer.deploy(getJoiningNodeName());
   }

   @Test
   @OperateOnDeployment(COORDINATOR_NODE)
   @InSequence(2)
   public void testCreateItemOnCoordinator() throws Exception {
      Cache<String, String> cache = getReplicatedCache(getCacheManager(getCoordinatorNodeConfig()));
      cache.put(TEST_ITEM_KEY, TEST_ITEM_VALUE);
      assertEquals(TEST_ITEM_VALUE, cache.get(TEST_ITEM_KEY));
   }

   @Test
   @OperateOnDeployment(JOINING_NODE)
   @InSequence(3)
   public void testReadItemOnJoiningNode() throws Exception {
      EmbeddedCacheManager manager = getCacheManager(getJoiningNodeConfig());  
      Cache<String, String> cache = getReplicatedCache(manager);
      assertEquals("Insufficient number of cluster members", 2, manager.getMembers().size());
      assertEquals(TEST_ITEM_VALUE, cache.get(TEST_ITEM_KEY));
   }

   @Test
   @InSequence(4)
   public void stopJoiningNodes() throws Exception {
      deployer.undeploy(getJoiningNodeName());
      deployer.undeploy(COORDINATOR_NODE);
      try {
         controller.stop(getJoiningNodeName());
      } catch(Exception e) {
         LOG.warn("Joining node stop failed with %s", e.getCause());
         controller.kill(getJoiningNodeName());
      }
      try {
         controller.stop(COORDINATOR_NODE);
      } catch(Exception e) {
         LOG.warn("Coordinator node stop failed with %s", e.getCause());
         controller.kill(COORDINATOR_NODE);
      }
      assertFalse(controller.isStarted(getJoiningNodeName()));
      assertFalse(controller.isStarted(COORDINATOR_NODE));
   }

}
