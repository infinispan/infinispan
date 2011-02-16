package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;
import static org.infinispan.distribution.BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete;
import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.distribution.DistributionTestHelper.isOwner;

/**
 * This tests the access pattern where a Tx touches multiple keys such that: K1: {A, B} K2: {A, C}
 * <p/>
 * The tx starts and runs on A, and the TX must succeed even though each node only gets a subset of data.  Particularly,
 * needs to be checked when using a cache store.
 */
@Test(testName = "distribution.DistCacheStoreTxDisjointSetTest", groups = "functional")
public class DistCacheStoreTxDisjointSetTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      c.setCacheLoaderManagerConfig(new CacheLoaderManagerConfig(new DummyInMemoryCacheStore.Cfg("DistCacheStoreTxDisjointSetTest0")));
      addClusterEnabledCacheManager(c, true);

      c.setCacheLoaderManagerConfig(new CacheLoaderManagerConfig(new DummyInMemoryCacheStore.Cfg("DistCacheStoreTxDisjointSetTest1")));
      addClusterEnabledCacheManager(c, true);

      c.setCacheLoaderManagerConfig(new CacheLoaderManagerConfig(new DummyInMemoryCacheStore.Cfg("DistCacheStoreTxDisjointSetTest2")));
      addClusterEnabledCacheManager(c, true);

      waitForInitRehashToComplete(cache(0),
                                  cache(1),
                                  cache(2)
      );
   }

   public void testDisjointSetTransaction() throws Exception {
      MagicKey k1 = new MagicKey(cache(0));
      MagicKey k2 = new MagicKey(cache(1));

      // make sure the owners of k1 and k2 are NOT the same!
      Set<Address> k1Owners = new HashSet<Address>();
      Set<Address> k2Owners = new HashSet<Address>();

      for (Cache<?, ?> cache: caches()) {
         if (isOwner(cache, k1)) k1Owners.add(addressOf(cache));
         if (isOwner(cache, k2)) k2Owners.add(addressOf(cache));
      }

      assert k1Owners.size() == 2: "Expected 2 owners for k1; was " + k1Owners;
      assert k2Owners.size() == 2: "Expected 2 owners for k1; was " + k2Owners;

      assert !k1Owners.equals(k2Owners) : format("k1 and k2 should have different ownership set.  Was %s and %s", k1Owners, k2Owners);

      tm(0).begin();
      cache(0).put(k1, "v1");
      cache(0).put(k2, "v2");
      tm(0).commit();
   }
}
