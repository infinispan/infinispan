package org.infinispan.test.integration.security.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.container.test.api.Deployer;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;

/**
 * @author vjuranek
 * @since 7.0
 */
public abstract class AbstractNodeAuthentication {

   protected static final String COORDINATOR_NODE = "node0";
   protected static final String COORDINATOR_JGROUSP_CONFIG_MD5 = "jgroups-tcp-sasl-md5-node0.xml";
   protected static final String COORDINATOR_JGROUSP_CONFIG_MD5_USER = "jgroups-tcp-sasl-md5-user-node0.xml";
   protected static final String JOINING_NODE_JGROUSP_CONFIG_MD5 = "jgroups-tcp-sasl-md5-node1.xml";
   protected static final String COORDINATOR_JGROUSP_CONFIG_KRB = "jgroups-tcp-sasl-krb-node0.xml";
   protected static final String JOINING_NODE_JGROUSP_CONFIG_KRB = "jgroups-tcp-sasl-krb-node1.xml";
   protected static final String JOINING_NODE_JGROUSP_CONFIG_KRB_FAIL = "jgroups-tcp-sasl-krb-node1-fail.xml";

   protected static final String CACHE_NAME = "replicatedCache";
   protected static final String TEST_ITEM_KEY = "test_key";
   protected static final String TEST_ITEM_VALUE = "test_value";
   
   private static final Log LOG = LogFactory.getLog(AbstractNodeAuthentication.class);

   @ArquillianResource
   protected ContainerController controller;

   @ArquillianResource
   protected Deployer deployer;

   protected abstract String getCoordinatorNodeConfig();

   protected abstract String getJoiningNodeName();

   protected abstract String getJoiningNodeConfig();
   
   protected Cache<String, String> getReplicatedCache(EmbeddedCacheManager manager) throws Exception {
      ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
      cacheConfig.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheConfig.invocationBatching().enable();
      cacheConfig.jmxStatistics().disable();
      cacheConfig.clustering().cacheMode(CacheMode.REPL_SYNC);

      manager.defineConfiguration(CACHE_NAME, cacheConfig.build());
      Cache<String, String> replicatedCache = manager.getCache(CACHE_NAME);
      
      return replicatedCache;
   }

   protected EmbeddedCacheManager getCacheManager(String jgrousConfigFile) {
      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder();
      globalConfig.globalJmxStatistics().disable();
      globalConfig.globalJmxStatistics().mBeanServerLookup(null); //TODO remove once WFLY-3124 is fixed, for now fail JMX registration
      globalConfig.transport().defaultTransport().addProperty("configurationFile", jgrousConfigFile);
      EmbeddedCacheManager manager = new DefaultCacheManager(globalConfig.build()); 
      return manager;
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
   @InSequence(3)
   //Needs to be overwritten in test class, which should add annotation @OperateOnDeployment(getJoiningNodeName())
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
