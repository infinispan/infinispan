package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;

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
            .lockAcquisitionTimeout(100);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2);
      c1 = cm1.getCache();
      c2 = cm2.getCache();
      waitForClusterToForm();
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
      InterceptorChain ic = TestingUtil.extractComponent(c2, InterceptorChain.class);
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

