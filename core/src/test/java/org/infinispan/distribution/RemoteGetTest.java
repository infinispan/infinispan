package org.infinispan.distribution;

import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.RemoteGetTest")
public class RemoteGetTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(TestDataSCI.INSTANCE, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false), 3);
      // make sure all caches are started...
      cache(0);
      cache(1);
      cache(2);
      waitForClusterToForm();
   }

   @SuppressWarnings("unchecked")
   private Cache<MagicKey, String> getCacheForAddress(Address a) {
      for (Cache<?, ?> c: caches())
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(a)) return (Cache<MagicKey, String>) c;
      return null;
   }

   @SuppressWarnings("unchecked")
   private Cache<MagicKey, String> getNonOwner(List<Address> a) {
      for (Cache<?, ?> c: caches())
         if (!a.contains(c.getAdvancedCache().getRpcManager().getAddress())) return (Cache<MagicKey, String>) c;
      return null;
   }

   public void testRemoteGet() {
      Cache<MagicKey, String> c1 = cache(0);
      Cache<MagicKey, String> c2 = cache(1);
      Cache<MagicKey, String> c3 = cache(2);
      MagicKey k = new MagicKey(c1, c2);

      List<Address> owners = extractCacheTopology(c1).getDistribution(k).writeOwners();

      assertTrue(owners.size() == 2, "Key should have 2 owners");

      Cache<MagicKey, String> owner1 = getCacheForAddress(owners.get(0));
      assertTrue(owner1 == c1);
      Cache<MagicKey, String> owner2 = getCacheForAddress(owners.get(1));
      assertTrue(owner2 == c2);
      Cache<MagicKey, String> nonOwner = getNonOwner(owners);
      assertTrue(nonOwner == c3);

      owner1.put(k, "value");
      assertEquals(nonOwner.get(k), "value");
   }

   public void testGetOfNonexistentKey() {
      Object v = cache(0).get("__ doesn't exist ___");
      assertNull(v, "Should get a null response");
   }

   public void testGetOfNonexistentKeyOnOwner() {
      MagicKey mk = new MagicKey("does not exist", cache(0));
      Object v = cache(0).get(mk);
      assertNull(v, "Should get a null response");
   }
}
