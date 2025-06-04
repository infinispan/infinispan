package org.infinispan.configuration.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.eviction.EvictionStrategy;
import org.testng.annotations.Test;

/**
 * Tests to ensure the L1 Configuration builder operates properly
 *
 * @author William Burns
 * @since 7.0
 */
@Test(groups = "functional", testName = "configuration.cache.L1ConfigurationBuilderTest")
public class L1ConfigurationBuilderTest {
   public void testDefaultsWhenEnabledOnly() {
      Configuration config = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).l1().enable().build();

      L1Configuration l1Config = config.clustering().l1();

      assertTrue(l1Config.enabled());
      assertEquals(l1Config.cleanupTaskFrequency(), TimeUnit.MINUTES.toMillis(1));
      assertEquals(l1Config.invalidationThreshold(), 0);
      assertEquals(l1Config.lifespan(), TimeUnit.MINUTES.toMillis(10));
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testL1WithExceptionEviction() {
      Configuration config = new ConfigurationBuilder()
            .clustering()
               .cacheMode(CacheMode.DIST_SYNC)
               .l1().enable()
            .memory()
               .whenFull(EvictionStrategy.EXCEPTION)
               .maxCount(10)
            .transaction()
               .transactionMode(org.infinispan.transaction.TransactionMode.TRANSACTIONAL)
            .build();
      assertNotNull(config);
   }
}
