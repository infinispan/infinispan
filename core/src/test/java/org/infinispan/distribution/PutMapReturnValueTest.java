package org.infinispan.distribution;

import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.PutMapReturnValueTest")
@InCacheMode({ CacheMode.DIST_SYNC, CacheMode.REPL_SYNC })
public class PutMapReturnValueTest extends MultipleCacheManagersTest {

   private AdvancedCache<Object, String> c1;
   private AdvancedCache<Object, String> c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(TestDataSCI.INSTANCE, getDefaultClusteredCacheConfig(cacheMode), 2);
      c1 = this.<Object, String>cache(0).getAdvancedCache();
      c2 = this.<Object, String>cache(1).getAdvancedCache();
   }

   public void testGetAndPutAll() {
      MagicKey k1 = new MagicKey(c1);
      MagicKey k2 = new MagicKey(c1);
      MagicKey k3 = new MagicKey(c2);
      MagicKey k4 = new MagicKey(c2);

      c1.put(k1, "v1-0");
      c2.put(k3, "v3-0");

      Map<Object, String> map = new HashMap<>();
      map.put(k1, "v1-1");
      map.put(k2, "v2-1");
      map.put(k3, "v3-1");
      map.put(k4, "v4-1");

      Map<Object, String> result = c1.getAndPutAll(map);
      assertNotNull(result);
      assertEquals(2, result.size());
      assertEquals("v1-0", result.get(k1));
      assertEquals("v3-0", result.get(k3));

      map.put(k1, "v1-2");
      map.put(k2, "v2-2");
      map.put(k3, "v3-2");
      map.put(k4, "v4-2");
      result = c1.getAndPutAll(map);
      assertNotNull(result);
      assertEquals(4, result.size());
      assertEquals("v1-1", result.get(k1));
      assertEquals("v2-1", result.get(k2));
      assertEquals("v3-1", result.get(k3));
      assertEquals("v4-1", result.get(k4));

      result = c1.getAll(map.keySet());
      assertEquals(4, result.size());
      assertEquals("v1-2", result.get(k1));
      assertEquals("v2-2", result.get(k2));
      assertEquals("v3-2", result.get(k3));
      assertEquals("v4-2", result.get(k4));
   }
}
