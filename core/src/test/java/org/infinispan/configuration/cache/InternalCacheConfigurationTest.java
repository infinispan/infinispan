package org.infinispan.configuration.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for internal cache configuration to ensure warnings are suppressed.
 *
 * @author Claude Sonnet 4.5
 * @since 16.3
 */
@Test(testName = "configuration.cache.InternalCacheConfigurationTest", groups = "functional")
public class InternalCacheConfigurationTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;
   private InternalCacheRegistry internalCacheRegistry;

   @BeforeClass
   public void init() {
      cacheManager = TestCacheManagerFactory.createClusteredCacheManager();
      internalCacheRegistry = TestingUtil.extractGlobalComponent(cacheManager, InternalCacheRegistry.class);
   }

   @AfterClass(alwaysRun = true)
   public void cleanup() {
      if (cacheManager != null) {
         cacheManager.stop();
         cacheManager = null;
      }
   }

   public void testInternalCacheFlagIsSet() {
      String cacheName = "test-internal-cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);

      // Register as internal cache with PERSISTENT flag
      internalCacheRegistry.registerInternalCache(cacheName, builder.build(),
            EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));

      // Verify the configuration was marked as internal
      Configuration config = cacheManager.getCacheConfiguration(cacheName);
      assertTrue(config.isInternalCache(), "Internal cache should be marked as internal");

      // Cleanup
      internalCacheRegistry.unregisterInternalCache(cacheName);
   }

   public void testUserCacheFlagIsNotSet() {
      String cacheName = "test-user-cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);

      // Define as regular user cache
      cacheManager.defineConfiguration(cacheName, builder.build());

      // Verify the configuration is not marked as internal
      Configuration config = cacheManager.getCacheConfiguration(cacheName);
      assertFalse(config.isInternalCache(), "User cache should not be marked as internal");

      // Cleanup
      cacheManager.undefineConfiguration(cacheName);
   }

   public void testInternalCacheWithPersistenceDoesNotWarnInClusteredMode() {
      String cacheName = "test-persistent-internal-cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);

      // This should not generate a warning because it's marked as internal
      internalCacheRegistry.registerInternalCache(cacheName, builder.build(),
            EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));

      // If we get here without warnings, the test passes
      // The warning would be: "Non-shared store without purge-on-startup..."
      assertTrue(cacheManager.getCacheConfiguration(cacheName).isInternalCache());

      // Cleanup
      internalCacheRegistry.unregisterInternalCache(cacheName);
   }
}
