package org.infinispan.distribution;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.InvalidationFailureTest")
public class InvalidationFailureTest extends MultipleCacheManagersTest {
   private Object k0;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      config.clustering().l1().enable().hash().numOwners(1);
      config.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      createCluster(TestDataSCI.INSTANCE, config, 2);
      final String cacheName = manager(0).getCacheManagerConfiguration().defaultCacheName().get();
      manager(0).defineConfiguration("second", config.build());
      manager(1).defineConfiguration("second", config.build());
      manager(0).startCaches(cacheName, "second");
      manager(1).startCaches(cacheName, "second");
      waitForClusterToForm(cacheName, "second");
      cache(0).put("k","v");
      cache(0,"second").put("k","v");
      assert cache(1).get("k").equals("v");
      assert cache(1, "second").get("k").equals("v");

      k0 = new MagicKey(cache(0));
   }

   public void testL1Invalidated() throws Exception {
      tm(1).begin();
      cache(1).put(k0,"v");
      cache(1, "second").put(k0,"v");
      assert !lockManager(1).isLocked(k0);
      assert !lockManager(1,"second").isLocked(k0);
      Transaction transaction = tm(1).suspend();

      tm(0).begin();
      log.info("Before the put");
      cache(0, "second").put(k0, "v1");
      cache(0).put(k0, "v2");
      try {
         tm(0).commit();
         log.info("After the Commit");
      } catch (Exception e) {
         log.error("Error during commit", e);
         assert false : "this should not fail even if the invalidation does";
      } finally {
         tm(1).resume(transaction);
         tm(1).rollback();
         assert !lockManager(0).isLocked(k0);
         assert !lockManager(0, "second").isLocked(k0);
         assert !lockManager(1).isLocked(k0);
         assert !lockManager(1, "second").isLocked(k0);
      }
   }
}
