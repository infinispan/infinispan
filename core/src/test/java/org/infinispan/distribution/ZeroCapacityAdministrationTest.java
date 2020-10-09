package org.infinispan.distribution;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Tests that it's possible to perform operations via EmbeddedCacheManagerAdmin when zero-capacity-node=true
 *
 * @author Ryan Emerson
 * @since 12.0
 */
@Test(groups = "functional", testName = "distribution.ch.ZeroCapacityAdministrationTest")
public class ZeroCapacityAdministrationTest extends MultipleCacheManagersTest {
   private static final String TEST_DIR = tmpDirectory(ZeroCapacityAdministrationTest.class.getSimpleName());

   private EmbeddedCacheManager node1;
   private EmbeddedCacheManager zeroCapacityNode;

   @Override
   protected void createCacheManagers() throws Throwable {
      String state1 = tmpDirectory(TEST_DIR, "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1);
      node1 = addClusterEnabledCacheManager(global1, new ConfigurationBuilder());

      String zeroState = tmpDirectory(TEST_DIR, "zero");
      GlobalConfigurationBuilder globalZero = statefulGlobalBuilder(zeroState).zeroCapacityNode(true);
      zeroCapacityNode = addClusterEnabledCacheManager(globalZero, new ConfigurationBuilder());
      waitForClusterToForm();
   }

   public void testDefineClusterConfiguration() {
      Configuration config = new ConfigurationBuilder().build();
      zeroCapacityNode.administration().createCache("zero-cache", config);
      zeroCapacityNode.administration().createTemplate("zero-template", config);
      assertNotNull(node1.getCache("zero-cache"));
      assertNotNull(node1.getCacheConfiguration("zero-template"));
   }

   private GlobalConfigurationBuilder statefulGlobalBuilder(String stateDirectory) {
      Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory).sharedPersistentLocation(stateDirectory).configurationStorage(ConfigurationStorage.OVERLAY);
      return global;
   }
}
