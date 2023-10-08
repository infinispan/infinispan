package org.infinispan.lock;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;

import org.infinispan.Cache;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.distribution.VersionedDistributionInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "lock.StaleLocksOnPrepareFailureTest", groups = "functional")
@CleanupAfterMethod
public class StaleLocksOnPrepareFailureTest extends MultipleCacheManagersTest {

   private static final int NUM_CACHES = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfg.clustering().hash().numOwners(NUM_CACHES)
         .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      for (int i = 0; i < NUM_CACHES; i++) {
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(TestDataSCI.INSTANCE, cfg);
         registerCacheManager(cm);
      }
      waitForClusterToForm();
   }

   public void testModsCommit() throws Exception {
      Cache<Object, Object> c1 = cache(0);
      Cache<Object, Object> c2 = cache(NUM_CACHES /2);

      // force the prepare command to fail on c2
      FailInterceptor interceptor = new FailInterceptor();
      interceptor.failFor(VersionedPrepareCommand.class);
      AsyncInterceptorChain ic = extractInterceptorChain(c2);
      ic.addInterceptorBefore(interceptor, VersionedDistributionInterceptor.class);

      MagicKey k1 = new MagicKey("k1", c1);

      tm(c1).begin();
      c1.put(k1, "v1");

      try {
         tm(c1).commit();
         assert false : "Commit should have failed";
      } catch (Exception e) {
         // expected
      }

      for (int i = 0; i < NUM_CACHES; i++) {
        assertEventuallyNotLocked(cache(i), k1);
      }
   }
}
