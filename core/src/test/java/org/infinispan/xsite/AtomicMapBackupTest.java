package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.AtomicMapBackupTest")
public class AtomicMapBackupTest extends AbstractTwoSitesTest {

   public AtomicMapBackupTest() {
      isLonBackupTransactional = true;
      use2Pc = true;
   }

   public void testAtomicMapBackup() {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache("LON", 0), "amKey");
      assert map.isEmpty();
      log.trace("Update is here");
      map.put("a", "fancyValue");
      assertEquals("fancyValue", map.get("a"));

      assertEquals("fancyValue", AtomicMapLookup.getAtomicMap(backup("LON"), "amKey").get("a"));
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }
}
