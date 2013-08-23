package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "container.versioning.DistL1WriteSkewTest", groups = "functional")
@CleanupAfterMethod
public class DistL1WriteSkewTest extends DistWriteSkewTest {
   @Override
   protected void decorate(ConfigurationBuilder builder) {
      // Enable L1
      builder.clustering().l1().enable();
      builder.clustering().sync().replTimeout(100, TimeUnit.MINUTES);
   }

   @Test
   public void testL1ValuePutCanExpire() {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);
      Cache<Object, Object> cache2 = cache(2);

      MagicKey hello = new MagicKey("hello", cache0, cache1);

      DistributionTestHelper.assertIsNotInL1(cache2, hello);

      // Auto-commit is true
      cache2.put(hello, "world 1");

      DistributionTestHelper.assertIsInL1(cache2, hello);
   }

   @Test
   public void testL1ValueGetCanExpire() {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);
      Cache<Object, Object> cache2 = cache(2);

      MagicKey hello = new MagicKey("hello", cache0, cache1);

      DistributionTestHelper.assertIsNotInL1(cache2, hello);

      // Auto-commit is true
      cache0.put(hello, "world 1");

      assertEquals("world 1", cache2.get(hello));

      DistributionTestHelper.assertIsInL1(cache2, hello);
   }

   public void testL1Enabled() {
      for (Cache cache : caches()) {
         AssertJUnit.assertTrue("L1 not enabled for " + address(cache),
                                cache.getCacheConfiguration().clustering().l1().enabled());
      }
   }
}
