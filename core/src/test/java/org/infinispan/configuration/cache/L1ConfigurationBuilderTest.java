package org.infinispan.configuration.cache;

import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

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
}
