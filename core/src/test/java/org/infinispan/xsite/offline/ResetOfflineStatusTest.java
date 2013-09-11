package org.infinispan.xsite.offline;

import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.BackupSenderImpl;
import org.infinispan.xsite.BaseSiteUnreachableTest;
import org.infinispan.xsite.OfflineStatus;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.offline.ResetOfflineStatusTest")
public class ResetOfflineStatusTest extends BaseSiteUnreachableTest {


   private static final int FAILURES = 8;

   public ResetOfflineStatusTest() {
      failures = FAILURES;
      lonBackupFailurePolicy = BackupFailurePolicy.FAIL;
   }

   public void testPutWithFailures() {

      ComponentRegistry registry = cache("LON", 0).getAdvancedCache().getComponentRegistry();
      Transport transport= registry.getComponent(Transport.class);
      DelegatingTransport delegatingTransport = new DelegatingTransport(transport);
      registry.getGlobalComponentRegistry().registerComponent(delegatingTransport, Transport.class);
      BackupSenderImpl bs = (BackupSenderImpl) registry.getComponent(BackupSender.class);
      registry.rewire();
      OfflineStatus offlineStatus = bs.getOfflineStatus("NYC");

      delegatingTransport.fail = true;
      for (int i = 0; i < FAILURES; i++) {
         try {
            cache("LON", 0).put("k" + i, "v" + i);
            fail("This should have failed");
         } catch (Exception e) {
            //expected
         }
      }

      for (int i = 0; i < FAILURES; i++) {
         cache("LON", 0).put("k" + i, "v" + i);
      }

      for (int i = 0; i < FAILURES; i++) {
         assertEquals("v" + i, cache("LON", 0).get("k" + i));
      }

      assertEquals(BackupSender.BringSiteOnlineResponse.BROUGHT_ONLINE, bs.bringSiteOnline("NYC"));

      for (int i = 0; i < FAILURES - 1; i++) {
         try {
            cache("LON", 0).put("k" + i, "v" + i);
            fail("This should have failed");
         } catch (Exception e) {
            //expected
         }
      }

      delegatingTransport.fail = false;
      assertEquals(FAILURES - 1, offlineStatus.getFailureCount());
      cache("LON", 0).put("ki", "vi"); //this should reset the offline status
      assertEquals(0, offlineStatus.getFailureCount());

      for (int i = 0; i < FAILURES * 10; i++) {
         cache("LON", 0).put("k" + i, "v" + i);
      }

      for (int i = 0; i < FAILURES * 10; i++) {
         assertEquals("v" + i, cache("LON", 0).get("k" + i));
      }
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }
}
