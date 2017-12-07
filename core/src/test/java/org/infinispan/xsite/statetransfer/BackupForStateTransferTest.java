package org.infinispan.xsite.statetransfer;

import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Simple test for the state transfer with different cache names.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.BackupForStateTransferTest")
public class BackupForStateTransferTest extends AbstractStateTransferTest {

   private static final String VALUE = "value";
   private static final String LON_BACKUP_CACHE_NAME = "lonBackup";

   public BackupForStateTransferTest() {
      super();
      this.implicitBackupCache = false;
   }

   public void testStateTransferWithClusterIdle(Method method) {
      takeSiteOffline();
      assertOffline();
      assertNoStateTransferInReceivingSite(LON_BACKUP_CACHE_NAME);
      assertNoStateTransferInSendingSite();

      //NYC is offline... lets put some initial data in
      //we have 2 nodes in each site and the primary owner sends the state. Lets try to have more key than the chunk
      //size in order to each site to send more than one chunk.
      final int amountOfData = chunkSize() * 4;
      for (int i = 0; i < amountOfData; ++i) {
         cache(LON, 0).put(k(method, i), VALUE);
      }

      //check if NYC is empty (LON backup cache)
      assertInSite(NYC, LON_BACKUP_CACHE_NAME, cache -> assertTrue(cache.isEmpty()));

      //check if NYC is empty (default cache)
      assertInSite(NYC, cache -> assertTrue(cache.isEmpty()));

      startStateTransfer();

      assertEventuallyStateTransferNotRunning();

      assertOnline(LON, NYC);

      //check if all data is visible (LON backup cache)
      assertInSite(NYC, LON_BACKUP_CACHE_NAME, cache -> {
         for (int i = 0; i < amountOfData; ++i) {
            assertEquals(VALUE, cache.get(k(method, i)));
         }
      });

      //check if NYC is empty (default cache)
      assertInSite(NYC, cache -> assertTrue(cache.isEmpty()));

      assertEventuallyNoStateTransferInReceivingSite(LON_BACKUP_CACHE_NAME);
      assertNoStateTransferInSendingSite();
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Override
   protected void adaptLONConfiguration(BackupConfigurationBuilder builder) {
      builder.site(NYC).stateTransfer().chunkSize(10);
   }

}
