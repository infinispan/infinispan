package org.infinispan.xsite.offline;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.BackupSenderImpl;
import org.infinispan.xsite.BaseSiteUnreachableTest;
import org.infinispan.xsite.OfflineStatus;
import org.junit.Assert;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.offline.NonTxOfflineTest")
public class NonTxOfflineTest extends BaseSiteUnreachableTest {

   private static final int FAILURES = 8;
   private static final Object[] KEYS = new Object[FAILURES];
   protected int nrRpcPerPut = 1;

   public NonTxOfflineTest() {
      failures = FAILURES;
      lonBackupFailurePolicy = BackupFailurePolicy.FAIL;
   }

   public void testPutWithFailures() {
      populateKeys(cache("LON", 0));
      BackupSenderImpl bs = (BackupSenderImpl) cache("LON", 0).getAdvancedCache().getComponentRegistry().getComponent(BackupSender.class);
      OfflineStatus nycStatus = bs.getOfflineStatus("NYC");

      for (int i = 0; i < FAILURES / nrRpcPerPut; i++) {
         try {
            assertEquals(BackupSender.BringSiteOnlineResponse.ALREADY_ONLINE, bs.bringSiteOnline("NYC"));
            cache("LON", 0).put(KEYS[i], "v" + i);
            fail("This should have failed");
         } catch (Exception e) {
            Assert.assertEquals(i + 1, nycStatus.getFailureCount());
         }
      }

      assert nycStatus.isOffline();

      for (int i = 0; i < FAILURES; i++) {
         cache("LON", 0).put(KEYS[i], "v" + i);
      }

      for (int i = 0; i < FAILURES; i++) {
         assertEquals("v" + i, cache("LON", 0).get(KEYS[i]));
      }

      assertEquals(BackupSender.BringSiteOnlineResponse.NO_SUCH_SITE, bs.bringSiteOnline("NO_SITE"));

      assertEquals(BackupSender.BringSiteOnlineResponse.BROUGHT_ONLINE, bs.bringSiteOnline("NYC"));

      for (int i = 0; i < FAILURES / nrRpcPerPut; i++) {
         try {
            cache("LON", 0).put(KEYS[i], "v" + i);
            fail("This should have failed");
         } catch (Exception e) {
            //expected
         }
      }
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   private void populateKeys(Cache primaryOwner) {
      for (int i = 0; i < KEYS.length; ++i) {
         KEYS[i] = new MagicKey("k" + i, primaryOwner);
      }
   }
}
