package org.infinispan.lock;


import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lock.configuration.ClusteredLockManagerConfiguration;
import org.infinispan.lock.configuration.ClusteredLockManagerConfigurationBuilder;
import org.infinispan.lock.configuration.Reliability;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.lock.impl.ClusteredLockModuleLifecycle;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionMode;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Configuration test
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
@Test(groups = "unit", testName = "clusteredLock.ConfigurationTest")
public class ConfigurationTest extends AbstractCacheTest {

   public void testDefaultConfiguration() {
      TestingUtil.withCacheManager(() -> buildCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder()),
            cacheManager -> {
               ClusteredLockManagerConfiguration configuration = ClusteredLockManagerConfigurationBuilder.defaultConfiguration();
               Configuration cacheConfiguration = getClusteredLockCacheConfiguration(cacheManager);
               assertLockAndCacheConfiguration(configuration, cacheConfiguration);
            });
   }

   public void testReliabilityAvailable() {
      final GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      final ClusteredLockManagerConfiguration config = builder.addModule(ClusteredLockManagerConfigurationBuilder.class)
            .reliability(Reliability.AVAILABLE).create();
      TestingUtil.withCacheManager(() -> buildCacheManager(builder), cacheManager -> {
         Configuration cacheConfiguration = getClusteredLockCacheConfiguration(cacheManager);
         assertLockAndCacheConfiguration(config, cacheConfiguration);
      });
   }

   public void testReliabilityConsistent() {
      final GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      final ClusteredLockManagerConfiguration config = builder.addModule(ClusteredLockManagerConfigurationBuilder.class)
            .reliability(Reliability.CONSISTENT).create();
      TestingUtil.withCacheManager(() -> buildCacheManager(builder), cacheManager -> {
         Configuration cacheConfiguration = getClusteredLockCacheConfiguration(cacheManager);
         assertLockAndCacheConfiguration(config, cacheConfiguration);
      });
   }

   public void testNumOwner() {
      final GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      final ClusteredLockManagerConfiguration config = builder.addModule(ClusteredLockManagerConfigurationBuilder.class)
            .numOwner(5).create();
      TestingUtil.withCacheManager(() -> buildCacheManager(builder), cacheManager -> {
         Configuration cacheConfiguration = getClusteredLockCacheConfiguration(cacheManager);
         assertLockAndCacheConfiguration(config, cacheConfiguration);
      });
   }

   public void testMinusOneNumberOfOwner() {
      final GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      final ClusteredLockManagerConfiguration config = builder.addModule(ClusteredLockManagerConfigurationBuilder.class)
            .numOwner(-1).create();
      TestingUtil.withCacheManager(() -> buildCacheManager(builder), cacheManager -> {
         Configuration cacheConfiguration = getClusteredLockCacheConfiguration(cacheManager);
         assertLockAndCacheConfiguration(config, cacheConfiguration);
      });
   }

   public void testInvalidReliability() {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      ClusteredLockManagerConfigurationBuilder clBuilder = builder.addModule(ClusteredLockManagerConfigurationBuilder.class);

      clBuilder.reliability(null);
      assertClusteredLockConfigurationException(builder);
   }

   public void testInvalidNumOwner() {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      ClusteredLockManagerConfigurationBuilder clBuilder = builder.addModule(ClusteredLockManagerConfigurationBuilder.class);

      clBuilder.numOwner(0);
      assertClusteredLockConfigurationException(builder);
   }

   private static Configuration getClusteredLockCacheConfiguration(EmbeddedCacheManager cacheManager) {
      return cacheManager.getCache(ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME).getCacheConfiguration();
   }

   private static EmbeddedCacheManager buildCacheManager(GlobalConfigurationBuilder builder) {
      return new DefaultCacheManager(builder.build());
   }

   private static void assertLockAndCacheConfiguration(ClusteredLockManagerConfiguration config,
                                                       Configuration cacheConfig) {
      assertEquals(config.numOwners() < 0 ? CacheMode.REPL_SYNC : CacheMode.DIST_SYNC, cacheConfig.clustering().cacheMode());
      if (config.numOwners() > 0) {
         assertEquals(config.numOwners(), cacheConfig.clustering().hash().numOwners());
      }
      assertEquals(config.reliability() == Reliability.CONSISTENT ? PartitionHandling.DENY_READ_WRITES : PartitionHandling.ALLOW_READ_WRITES,
            cacheConfig.clustering().partitionHandling().whenSplit());
      assertFalse(cacheConfig.clustering().l1().enabled());
      assertEquals(TransactionMode.NON_TRANSACTIONAL, cacheConfig.transaction().transactionMode());
   }

   private void assertClusteredLockConfigurationException(GlobalConfigurationBuilder builder) {
      try {
         builder.build();
         AssertJUnit.fail("CacheConfigurationExpected");
      } catch (ClusteredLockException | CacheConfigurationException expected) {
         log.trace("Expected", expected);
      }
   }
}
