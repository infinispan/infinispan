package org.infinispan.xsite;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.BackupCacheStoppedTest")
public class BackupCacheStoppedTest extends AbstractTwoSitesTest {

   public void testCacheStopped() {
      final String site = "LON";
      String key = key(site);
      String val = val(site);

      cache(site, 0).put(key, val);
      Cache<Object,Object> backup = backup(site);
      final GlobalComponentRegistry gcr = backup.getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry();

      assertEquals(backup.get(key), val);
      assertTrue(backup.getStatus().allowInvocations());

      backup.stop();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            BackupReceiverRepositoryImpl component = (BackupReceiverRepositoryImpl) gcr.getComponent(BackupReceiverRepository.class);
            return component.get(site, EmbeddedCacheManager.DEFAULT_CACHE_NAME) == null;
         }
      });

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

