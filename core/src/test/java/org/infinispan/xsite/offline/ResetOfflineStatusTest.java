package org.infinispan.xsite.offline;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.BaseSiteUnreachableTest;
import org.infinispan.xsite.OfflineStatus;
import org.infinispan.xsite.status.BringSiteOnlineResponse;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.offline.ResetOfflineStatusTest")
public class ResetOfflineStatusTest extends BaseSiteUnreachableTest {


   private static final int FAILURES = 8;
   private static final Object[] KEYS = new Object[FAILURES * 10];

   public ResetOfflineStatusTest() {
      failures = FAILURES;
      lonBackupFailurePolicy = BackupFailurePolicy.FAIL;
   }

   public void testPutWithFailures() {
      populateKeys(cache(LON, 0));
      EmbeddedCacheManager manager = cache(LON, 0).getCacheManager();
      Transport transport = GlobalComponentRegistry.componentOf(manager, Transport.class);
      DelegatingTransport delegatingTransport = new DelegatingTransport(transport);
      TestingUtil.replaceComponent(manager, Transport.class, delegatingTransport, true);

      DefaultTakeOfflineManager tom = takeOfflineManager(LON, 0);
      OfflineStatus offlineStatus = tom.getOfflineStatus(NYC);

      delegatingTransport.fail = true;
      for (int i = 0; i < FAILURES; i++) {
         try {
            cache(LON, 0).put(KEYS[i], "v" + i);
            fail("This should have failed");
         } catch (Exception e) {
            //expected
         }
      }

      for (int i = 0; i < FAILURES; i++) {
         cache(LON, 0).put(KEYS[i], "v" + i);
      }

      for (int i = 0; i < FAILURES; i++) {
         assertEquals("v" + i, cache(LON, 0).get(KEYS[i]));
      }

      assertEquals(BringSiteOnlineResponse.BSOR_BROUGHT_ONLINE, tom.bringSiteOnline(NYC));

      for (int i = 0; i < FAILURES - 1; i++) {
         try {
            cache(LON, 0).put(KEYS[i], "v" + i);
            fail("This should have failed");
         } catch (Exception e) {
            //expected
         }
      }

      delegatingTransport.fail = false;
      assertEquals(FAILURES - 1, offlineStatus.getFailureCount());
      cache(LON, 0).put(KEYS[FAILURES], "vi"); //this should reset the offline status
      assertEquals(0, offlineStatus.getFailureCount());

      for (int i = 0; i < FAILURES * 10; i++) {
         cache(LON, 0).put(KEYS[i], "v" + i);
      }

      for (int i = 0; i < FAILURES * 10; i++) {
         assertEquals("v" + i, cache(LON, 0).get(KEYS[i]));
      }
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   private static void populateKeys(Cache primaryOwner) {
      for (int i = 0; i < KEYS.length; ++i) {
         KEYS[i] = new MagicKey("k" + i, primaryOwner);
      }
   }

}
