package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.NoTxDistCacheOperationsTest")
public class NoTxDistCacheOperationsTest extends BaseCacheOperationsTest {

   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   public void testDataGetsReplicated() {
      cache("LON", 0).put("k_lon", "v_lon");
      assertNull(cache("NYC", 0).get("k_lon"));
      assertEquals(cache("LON", 1).get("k_lon"), "v_lon");
      assertEquals(cache("NYC", "lonBackup", 0).get("k_lon"), "v_lon");
      assertEquals(cache("NYC", "lonBackup", 1).get("k_lon"), "v_lon");

      cache("NYC",1).put("k_nyc", "v_nyc");
      assertEquals(cache("LON", 1).get("k_lon"), "v_lon");
      assertEquals(cache("LON", "nycBackup", 0).get("k_nyc"), "v_nyc");
      assertEquals(cache("LON", "nycBackup", 1).get("k_nyc"), "v_nyc");
      assertNull(cache("LON", 0).get("k_nyc"));

      cache("LON", 1).remove("k_lon");
      assertNull(cache("LON", 1).get("k_lon"));
      assertNull(cache("NYC", "lonBackup", 0).get("k_lon"));
      assertNull(cache("NYC", "lonBackup", 1).get("k_lon"));
   }
}
