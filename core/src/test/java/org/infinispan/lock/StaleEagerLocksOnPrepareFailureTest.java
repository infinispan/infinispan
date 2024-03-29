package org.infinispan.lock;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.Assert.assertNull;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test(testName = "lock.StaleEagerLocksOnPrepareFailureTest", groups = "functional")
@CleanupAfterMethod
public class StaleEagerLocksOnPrepareFailureTest extends MultipleCacheManagersTest {

   Cache<MagicKey, String> c1, c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfg
         .transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .useSynchronization(false)
            .recovery()
               .disable()
         .locking()
            .lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      createCluster(TestDataSCI.INSTANCE, cfg, 2);
      waitForClusterToForm();
      c1 = cache(0);
      c2 = cache(1);
   }

   public void testNoModsCommit() throws Exception {
      doTest(false);
   }

   public void testModsCommit() throws Exception {
      doTest(true);
   }

   private void doTest(boolean mods) throws Exception {
      // force the prepare command to fail on c2
      FailInterceptor interceptor = new FailInterceptor();
      interceptor.failFor(PrepareCommand.class);
      AsyncInterceptorChain ic = extractInterceptorChain(c2);
      ic.addInterceptorBefore(interceptor, TxDistributionInterceptor.class);

      MagicKey k1 = new MagicKey("k1", c1);
      MagicKey k2 = new MagicKey("k2", c2);

      tm(c1).begin();
      if (mods) {
         c1.put(k1, "v1");
         c1.put(k2, "v2");

         assertKeyLockedCorrectly(k1);
         assertKeyLockedCorrectly(k2);
      } else {
         c1.getAdvancedCache().lock(k1);
         c1.getAdvancedCache().lock(k2);

         assertNull(c1.get(k1));
         assertNull(c1.get(k2));

         assertKeyLockedCorrectly(k1);
         assertKeyLockedCorrectly(k2);
      }

      try {
         tm(c1).commit();
         assert false : "Commit should have failed";
      } catch (Exception e) {
         // expected
      }

      assertEventuallyNotLocked(c1, k1);
      assertEventuallyNotLocked(c2, k1);
      assertEventuallyNotLocked(c1, k2);
      assertEventuallyNotLocked(c2, k2);
   }
}
