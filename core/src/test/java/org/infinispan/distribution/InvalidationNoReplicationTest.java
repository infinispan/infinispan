package org.infinispan.distribution;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.InvalidationNoReplicationTest")
public class InvalidationNoReplicationTest extends MultipleCacheManagersTest {

   private Object k0;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      config.setL1CacheEnabled(true);
      config.setNumOwners(1);
      createCluster(config, 2);
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));
      KeyAffinityService<Object> service = KeyAffinityServiceFactory.
            newKeyAffinityService(cache(0), Executors.newSingleThreadExecutor(), new RndKeyGenerator(), 2, true);
      k0 = service.getKeyForAddress(address(0));
      service.stop();
   }

   public void testInvalidationWithTx() throws Exception {
      advancedCache(1).withFlags(Flag.CACHE_MODE_LOCAL).put(k0, "k1");
      assert advancedCache(1).getDataContainer().containsKey(k0);
      assert !advancedCache(0).getDataContainer().containsKey(k0);

      tm(0).begin();
      cache(0).put(k0, "v2");
      tm(0).commit();

      assert !advancedCache(1).getDataContainer().containsKey(k0);
   }

   public void testInvalidationNoTx() throws Exception {
      cache(1).put(k0, "v0");
      assert advancedCache(0).getDataContainer().containsKey(k0);
      assert advancedCache(1).getDataContainer().containsKey(k0);

      log.info("Here is the put!");
      log.info("Cache 0=%s cache 1=%s", address(0), address(1));
      cache(0).put(k0, "v1");

      log.info("before assertions!");
      assertEquals(advancedCache(1).getDataContainer().get(k0), null);
      assertEquals(advancedCache(0).getDataContainer().get(k0).getValue(), "v1");
   }

}
