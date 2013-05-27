package org.infinispan.stats.topK;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusterTopKeyTest;
import org.testng.annotations.Test;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.topK.ReplTopKeyTest")
public class ReplTopKeyTest extends BaseClusterTopKeyTest {

   public ReplTopKeyTest() {
      super(CacheMode.REPL_SYNC, 2);
   }

   @Override
   protected boolean isOwner(Cache<?, ?> cache, Object key) {
      return true;
   }

   @Override
   protected boolean isPrimaryOwner(Cache<?, ?> cache, Object key) {
      return cache.getAdvancedCache().getRpcManager().getTransport().getCoordinator().equals(addressOf(cache));
   }
}
