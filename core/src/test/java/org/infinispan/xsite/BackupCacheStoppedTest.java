package org.infinispan.xsite;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.util.ByteString.fromString;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.GlobalInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
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
      Cache<Object,Object> backup = backup(site);

      assertEquals(backup.get(key), val);
      assertTrue(backup.getStatus().allowInvocations());

      GlobalInboundInvocationHandler handler = (GlobalInboundInvocationHandler) extractGlobalComponent(backup.getCacheManager(), InboundInvocationHandler.class);

      backup.stop();
      eventually(() -> handler.getLocalCacheForRemoteSite(site, fromString(getDefaultCacheName())) == null);

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
