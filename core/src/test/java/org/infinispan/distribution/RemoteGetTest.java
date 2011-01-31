package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups = "functional", testName = "distribution.RemoteGetTest")
public class RemoteGetTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(Configuration.CacheMode.DIST_SYNC, 3);
      // make sure all caches are started...
      cache(0);
      cache(1);
      cache(2);
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
      MagicKey k = new MagicKey(cache(0)); // this should now map to cache0 and cache1

      List<Address> owners = cache(0).getAdvancedCache().getDistributionManager().locate(k);

      assert owners.size() == 2: "Key should have 2 owners";

      Cache<MagicKey, String> owner1 = getCacheForAddress(owners.get(0));
      Cache<MagicKey, String> owner2 = getCacheForAddress(owners.get(1));
      Cache<MagicKey, String> nonOwner = getNonOwner(owners);

      owner1.put(k, "value");
      assert "value".equals(nonOwner.get(k));
   }

   public void testGetOfNonexistentKey() {
      Object v = cache(0).get("__ doesn't exist ___");
      assert v == null : "Should get a null response";
   }
}
