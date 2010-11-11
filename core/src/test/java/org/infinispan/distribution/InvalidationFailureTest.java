package org.infinispan.distribution;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.concurrent.Executors;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.InvalidationFailureTest")
public class InvalidationFailureTest extends MultipleCacheManagersTest {
   private Object k0;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      config.setL1CacheEnabled(true);
      config.setNumOwners(1);
      createCluster(config, 2);
      manager(0).defineConfiguration("second", config);
      manager(1).defineConfiguration("second", config);
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));
      TestingUtil.blockUntilViewsReceived(10000, cache(0, "second"), cache(1, "second"));
      cache(0).put("k","v");
      cache(0,"second").put("k","v");
      assert cache(1).get("k").equals("v");
      assert cache(1, "second").get("k").equals("v");

      KeyAffinityService<Object> service = KeyAffinityServiceFactory.newKeyAffinityService(cache(0),
                                                                                           Executors.newSingleThreadExecutor(),
                                                                                           new RndKeyGenerator(), 2, true);
      k0 = service.getKeyForAddress(address(0));
      service.stop();
   }

   public void testH1Invalidated() throws Exception {
      tm(1).begin();
      cache(1).put(k0,"v");
      cache(1, "second").put(k0,"v");
      assert lockManager(1).isLocked(k0);
      assert lockManager(1,"second").isLocked(k0);
      Transaction transaction = tm(1).suspend();

      tm(0).begin();
      log.info("Before the put");
      cache(0, "second").put(k0, "v1");
      cache(0).put(k0, "v2");
      try {
         tm(0).commit();
         log.info("After the Commit");
      } catch (Exception e) {
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
