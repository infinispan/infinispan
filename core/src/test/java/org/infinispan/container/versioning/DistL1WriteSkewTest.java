package org.infinispan.container.versioning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

@Test(testName = "container.versioning.DistL1WriteSkewTest", groups = "functional")
public class DistL1WriteSkewTest extends DistWriteSkewTest {
   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationBuilder() {
      GlobalConfigurationBuilder builder = super.defaultGlobalConfigurationBuilder();
      builder.transport().distributedSyncTimeout(5, TimeUnit.MINUTES);
      return builder;
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      // Enable L1
      builder.clustering().l1().enable();
      builder.clustering().remoteTimeout(4, TimeUnit.MINUTES);
      builder.clustering().stateTransfer().timeout(5, TimeUnit.MINUTES);
      builder.transaction().completedTxTimeout(TimeUnit.MINUTES.toMillis(5));
   }

   @Test
   public void testL1ValuePutCanExpire() {
      Cache<Object, Object> cache0 = cache(0);
      MagicKey hello = new MagicKey("hello", cache0);
      Cache<Object, Object> nonOwner = DistributionTestHelper.getFirstNonOwner(hello, caches());

      DistributionTestHelper.assertIsNotInL1(nonOwner, hello);

      // Auto-commit is true
      nonOwner.put(hello, "world 1");

      DistributionTestHelper.assertIsInL1(nonOwner, hello);
   }

   @Test
   public void testL1ValueGetCanExpire() {
      Cache<Object, Object> cache0 = cache(0);
      MagicKey hello = new MagicKey("hello", cache0);
      Cache<Object, Object> nonOwner = DistributionTestHelper.getFirstNonOwner(hello, caches());

      DistributionTestHelper.assertIsNotInL1(nonOwner, hello);

      // Auto-commit is true
      cache0.put(hello, "world 1");

      assertEquals("world 1", nonOwner.get(hello));

      DistributionTestHelper.assertIsInL1(nonOwner, hello);
   }

   public void testL1Enabled() {
      for (Cache cache : caches()) {
         assertTrue(cache.getCacheConfiguration().clustering().l1().enabled(), "L1 not enabled for " + address(cache));
      }
   }
}
