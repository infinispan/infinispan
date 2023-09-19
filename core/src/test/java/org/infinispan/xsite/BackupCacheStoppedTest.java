package org.infinispan.xsite;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.util.ByteString.fromString;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.BackupCacheStoppedTest")
public class BackupCacheStoppedTest extends AbstractTwoSitesTest {

   public void testCacheStopped() {
      final String site = LON;
      String key = key(site);
      String val = val(site);

      cache(site, 0).put(key, val);
      var lonCacheName = fromString(cache(site, 0).getName());

      Cache<Object,Object> backup = backup(site);
      var nycCacheName = fromString(backup.getName());

      assertEquals(backup.get(key), val);
      assertTrue(backup.getStatus().allowInvocations());

      XSiteCacheMapper mapper = extractGlobalComponent(backup.getCacheManager(), XSiteCacheMapper.class);
      // mapping should exist
      assertEquals(nycCacheName, mapper.peekLocalCacheForRemoteSite(site, lonCacheName)
            .map(XSiteCacheMapper.LocalCacheInfo::cacheName)
            .orElse(null));

      backup.stop();
      eventually(() -> mapper.peekLocalCacheForRemoteSite(site, lonCacheName).isEmpty());

      assertFalse(backup.getStatus().allowInvocations());

      backup.start();

      log.trace("About to put the 2nd value");
      cache(site, 0).put(key, "v2");
      assertEquals(backup(site).get(key), "v2");
      assertTrue(backup.getStatus().allowInvocations());
   }

   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }
}
