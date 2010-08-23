package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "distribution.rehash.RehashLeaveTestBase")
public abstract class RehashLeaveTestBase extends RehashTestBase {
   void waitForRehashCompletion() {
      long giveupTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60 * 2);


      for (Cache c : caches) {
         DistributionManager distributionManager = getDistributionManager(c);

         while (distributionManager.isRehashInProgress() && System.currentTimeMillis() < giveupTime) {
            TestingUtil.sleepThread(250);
         }

         if (distributionManager.isRehashInProgress())
            throw new RuntimeException("Timed out waiting for rehash to complete on cache " + addressOf(c));
      }

      // ensure transaction logging is disabled everywhere
      for (Cache c : caches)
         assert !getDistributionManager(c).getTransactionLogger().isEnabled() : "Transaction logging for cache " + addressOf(c) + " is still enabled!!";
   }
}
