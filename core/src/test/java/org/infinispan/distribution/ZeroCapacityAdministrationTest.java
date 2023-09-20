package org.infinispan.distribution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.globalstate.GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME;
import static org.infinispan.globalstate.impl.GlobalConfigurationManagerImpl.CACHE_SCOPE;
import static org.testng.AssertJUnit.assertNotNull;

import java.nio.file.Paths;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.logging.Log;
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
      String state1 = Paths.get(TEST_DIR, "1").toString();
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1);
      node1 = addClusterEnabledCacheManager(global1, new ConfigurationBuilder());

      String zeroState = Paths.get(TEST_DIR, "zero").toString();
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

   public void testCreateNewClusteredCacheFromZeroToRemote() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .build();

      // The join command is sent to the coordinator.
      assertThat(node1.isCoordinator()).isTrue();

      // Make sure this will trigger a remote call to create the cache.
      int tries = 0;
      String cacheName = "another-cache";
      ScopedState ss = new ScopedState(CACHE_SCOPE, cacheName);
      while (!DistributionTestHelper.isFirstOwner(node1.getCache(CONFIG_STATE_CACHE_NAME), ss)) {
         if (tries > 50) fail("Exceeded attempts to find configuration mapping to remote");

         cacheName = "another-cache-" + tries++;
         ss = new ScopedState(CACHE_SCOPE, cacheName);
      }

      Log.CONFIG.infof("Registering the '%s' cache, lets go!", cacheName);
      Cache<?, ?> cache = zeroCapacityNode.administration().getOrCreateCache(cacheName, config);
      assertNotNull(cache);
   }

   private GlobalConfigurationBuilder statefulGlobalBuilder(String stateDirectory) {
      Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory).sharedPersistentLocation(stateDirectory).configurationStorage(ConfigurationStorage.OVERLAY);
      return global;
   }
}
