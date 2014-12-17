package org.infinispan.xsite.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.xsite.AbstractXSiteTest;
import org.infinispan.xsite.XSiteAdminOperations;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests the {@link org.infinispan.xsite.BackupReceiver} for the local caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.LocalCacheStateTransferTest")
public class LocalCacheStateTransferTest extends AbstractXSiteTest {


   private static final String LON = "LON";
   private static final String NYC = "NYC";

   public void testStateTransferWithClusterIdle() throws InterruptedException {
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);

      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);

      //NYC is offline... lets put some initial data in
      //we have 2 nodes in each site and the primary owner sends the state. Lets try to have more key than the chunk
      //size in order to each site to send more than one chunk.
      final int amountOfData = chunkSize(LON) * 4;
      for (int i = 0; i < amountOfData; ++i) {
         cache(LON, 0).put(key(i), value(0));
      }

      //check if NYC is empty (LON backup cache)
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertTrue(cache.isEmpty());
         }
      });

      //check if NYC is empty (default cache)
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertTrue(cache.isEmpty());
         }
      });

      startStateTransfer(LON, NYC);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return extractComponent(cache(LON, 0), XSiteAdminOperations.class).getRunningStateTransfer().isEmpty();
         }
      }, TimeUnit.SECONDS.toMillis(30));

      assertOnline(LON, NYC);

      //check if all data is visible (LON backup cache)
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            for (int i = 0; i < amountOfData; ++i) {
               assertEquals(value(0), cache.get(key(i)));
            }
         }
      });

      //check if all data is visible NYC
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            for (int i = 0; i < amountOfData; ++i) {
               assertEquals(value(0), cache.get(key(i)));
            }
         }
      });

      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);
   }

   @Override
   protected void createSites() {
      createSite(LON, 1, globalConfigurationBuilderForSite(LON), configurationBuilderForSite(NYC));
      createSite(NYC, 1, globalConfigurationBuilderForSite(NYC), TestCacheManagerFactory.getDefaultCacheConfiguration(false));
   }

   private GlobalConfigurationBuilder globalConfigurationBuilderForSite(String siteName) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.site().localSite(siteName);
      return builder;
   }

   private ConfigurationBuilder configurationBuilderForSite(String backupSiteName) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.sites().addBackup().site(backupSiteName)
            .stateTransfer().chunkSize(1);
      return builder;
   }

   private void startStateTransfer(String fromSite, String toSite) {
      XSiteAdminOperations operations = extractComponent(cache(fromSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.SUCCESS, operations.pushState(toSite));
   }

   private void takeSiteOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.SUCCESS, operations.takeSiteOffline(remoteSite));
   }

   private void assertOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.OFFLINE, operations.siteStatus(remoteSite));
   }

   private void assertOnline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.ONLINE, operations.siteStatus(remoteSite));
   }

   private int chunkSize(String site) {
      return cache(site, 0).getCacheConfiguration().sites().allBackups().get(0).stateTransfer().chunkSize();
   }

   private void assertNoStateTransferInReceivingSite(String siteName) {
      Cache<?, ?> cache = cache(siteName, 0);
      CommitManager commitManager = extractComponent(cache, CommitManager.class);
      assertFalse(commitManager.isTracking(Flag.PUT_FOR_STATE_TRANSFER));
      assertFalse(commitManager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));
      assertTrue(commitManager.isEmpty());
   }

   private void assertNoStateTransferInSendingSite(String siteName) {
      Cache<?, ?> cache = cache(siteName, 0);
      assertTrue(extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty());
   }

   private Object key(int index) {
      return "key-" + index;
   }

   private Object value(int index) {
      return "value-" + index;
   }
}
