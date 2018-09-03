package org.infinispan.xsite;

import static java.lang.String.format;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.util.NotifierLatch;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test for {@link GlobalXSiteAdminOperations}.
 *
 * @author Pedro Ruivo
 * @since 8.1
 */
@CleanupAfterTest
@Test(groups = "xsite", testName = "xsite.GlobalXSiteAdminOpsTest")
public class GlobalXSiteAdminOpsTest extends AbstractMultipleSitesTest {



   protected static ConfigurationBuilder newConfiguration() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }

   public void testTakeSiteOffline(Method m) {
      final String key = k(m);
      final String value = v(m);

      assertAllCachesEmpty();
      assertSiteStatusInAllCaches(XSiteAdminOperations.ONLINE);

      extractGlobalComponent(site(0).cacheManagers().get(0), GlobalXSiteAdminOperations.class).takeSiteOffline(siteName(1));

      assertSiteStatus(0, 0, null, 1, XSiteAdminOperations.OFFLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 1, XSiteAdminOperations.OFFLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 2, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_2.name(), 2, XSiteAdminOperations.ONLINE);

      //double check with data
      putInAllCache(key, value);
      assertValueInAllCachesInPrimarySite(key, value); //all caches should have the value in primary site

      assertCacheEmpty(1, 0, null);
      assertCacheEmpty(1, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertValueInCache(2, CacheType.BACKUP_TO_SITE_1_AND_2.name(), key, value);
      assertValueInCache(2, CacheType.BACKUP_TO_SITE_2.name(), key, value);
   }

   public void testBringSiteOnline(Method m) {
      final String key = k(m);
      final String value = v(m);

      assertAllCachesEmpty();
      setSitesStatus(false);
      assertSiteStatusInAllCaches(XSiteAdminOperations.OFFLINE);

      extractGlobalComponent(site(0).cacheManagers().get(0), GlobalXSiteAdminOperations.class).bringSiteOnline(siteName(1));

      assertSiteStatus(0, 0, null, 1, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 1, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 2, XSiteAdminOperations.OFFLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_2.name(), 2, XSiteAdminOperations.OFFLINE);

      //double check with data
      putInAllCache(key, value);
      assertValueInAllCachesInPrimarySite(key, value); //all caches should have the value in primary site

      assertValueInCache(1, null, key, value);
      assertValueInCache(1, CacheType.BACKUP_TO_SITE_1_AND_2.name(), key, value);
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_2.name());
   }

   public void testPushState(Method m) {
      final String key = k(m);
      final String value = v(m);

      assertAllCachesEmpty();
      setSitesStatus(false);
      assertSiteStatusInAllCaches(XSiteAdminOperations.OFFLINE);

      putInAllCache(key, value);
      assertValueInAllCachesInPrimarySite(key, value);

      //check the value is not in the backups
      assertCacheEmpty(1, 0, null);
      assertCacheEmpty(1, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_2.name());

      extractGlobalComponent(site(0).cacheManagers().get(0), GlobalXSiteAdminOperations.class).pushState(siteName(1));
      awaitXSiteStateTransfer();

      //check state and data
      assertSiteStatus(0, 0, null, 1, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 1, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 2, XSiteAdminOperations.OFFLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_2.name(), 2, XSiteAdminOperations.OFFLINE);

      assertValueInCache(1, null, key, value);
      assertValueInCache(1, CacheType.BACKUP_TO_SITE_1_AND_2.name(), key, value);
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_2.name());

      extractGlobalComponent(site(0).cacheManagers().get(0), GlobalXSiteAdminOperations.class).pushState(siteName(2));
      awaitXSiteStateTransfer();

      //check state and data
      assertSiteStatus(0, 0, null, 1, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 1, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 2, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_2.name(), 2, XSiteAdminOperations.ONLINE);

      assertValueInCache(1, null, key, value);
      assertValueInCache(1, CacheType.BACKUP_TO_SITE_1_AND_2.name(), key, value);
      assertValueInCache(2, CacheType.BACKUP_TO_SITE_1_AND_2.name(), key, value);
      assertValueInCache(2, CacheType.BACKUP_TO_SITE_2.name(), key, value);
   }

   public void testCancelPushState(Method m) {
      final String key = k(m);
      final String value = v(m);

      assertAllCachesEmpty();
      setSitesStatus(false);
      assertSiteStatusInAllCaches(XSiteAdminOperations.OFFLINE);

      putInAllCache(key, value);
      assertValueInAllCachesInPrimarySite(key, value);

      //check the value is not in the backups
      assertCacheEmpty(1, 0, null);
      assertCacheEmpty(1, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_2.name());

      List<BlockingTransport> blockingTransportList = getBlockingTransport(0, true);
      blockingTransportList.forEach(BlockingTransport::blockCommands);

      extractGlobalComponent(site(0).cacheManagers().get(0), GlobalXSiteAdminOperations.class).pushState(siteName(1));

      extractGlobalComponent(site(0).cacheManagers().get(0), GlobalXSiteAdminOperations.class).cancelPushState(siteName(1));

      //check state and data
      assertSiteStatus(0, 0, null, 1, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 1, XSiteAdminOperations.ONLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 2, XSiteAdminOperations.OFFLINE);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_2.name(), 2, XSiteAdminOperations.OFFLINE);

      assertCacheEmpty(1, 0, null);
      assertCacheEmpty(1, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_2.name());

      blockingTransportList.forEach(BlockingTransport::unblockCommands);
   }

   @AfterMethod(alwaysRun = true)
   public void resetStatusAfterMethod() {
      setSitesStatus(true);
      getBlockingTransport(0, false).forEach(BlockingTransport::unblockCommands);
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      if (siteIndex == 0) {
         //default cache will backup to site_1
         ConfigurationBuilder builder = newConfiguration();
         builder.sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.SYNC);
         return builder;
      } else {
         return newConfiguration();
      }
   }

   @Override
   protected int defaultNumberOfSites() {
      return 3;
   }

   @Override
   protected void afterSitesCreated() {
      super.afterSitesCreated();

      ConfigurationBuilder builder = newConfiguration();
      builder.sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      defineInSite(site(0), CacheType.BACKUP_TO_SITE_2.name(), builder.build());
      defineInSite(site(2), CacheType.BACKUP_TO_SITE_2.name(), newConfiguration().build());

      builder = newConfiguration();
      builder.sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      builder.sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      defineInSite(site(0), CacheType.BACKUP_TO_SITE_1_AND_2.name(), builder.build());
      defineInSite(site(1), CacheType.BACKUP_TO_SITE_1_AND_2.name(), newConfiguration().build());
      defineInSite(site(2), CacheType.BACKUP_TO_SITE_1_AND_2.name(), newConfiguration().build());

      defineInSite(site(0), CacheType.NO_BACKUP.name(), newConfiguration().build());

      //wait for caches in primary cluster
      site(0).waitForClusterToForm(null);
      site(0).waitForClusterToForm(CacheType.BACKUP_TO_SITE_1_AND_2.name());
      site(0).waitForClusterToForm(CacheType.BACKUP_TO_SITE_1_AND_2.name());
      site(0).waitForClusterToForm(CacheType.BACKUP_TO_SITE_2.name());

      //wait for caches in backup site 1
      site(1).waitForClusterToForm(null);
      site(1).waitForClusterToForm(CacheType.BACKUP_TO_SITE_1_AND_2.name());

      //wait for caches in backup site 2
      site(2).waitForClusterToForm(CacheType.BACKUP_TO_SITE_1_AND_2.name());
      site(2).waitForClusterToForm(CacheType.BACKUP_TO_SITE_2.name());
   }

   private void awaitXSiteStateTransfer() {
      awaitXSiteStateTransferFor(null);
      awaitXSiteStateTransferFor(CacheType.BACKUP_TO_SITE_1_AND_2.name());
      awaitXSiteStateTransferFor(CacheType.BACKUP_TO_SITE_2.name());
   }

   private void awaitXSiteStateTransferFor(String cacheName) {
      eventually(format("Failed to complete the x-site state transfer for cache '%s'", cacheName),
                 () -> xSiteAdminOperations(0, 0, cacheName).getRunningStateTransfer().isEmpty());
   }

   private void setSitesStatus(boolean online) {
      if (online) {
         xSiteAdminOperations(0, 0, null).bringSiteOnline(siteName(1));
         xSiteAdminOperations(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name()).bringSiteOnline(siteName(1));
         xSiteAdminOperations(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name()).bringSiteOnline(siteName(2));
         xSiteAdminOperations(0, 0, CacheType.BACKUP_TO_SITE_2.name()).bringSiteOnline(siteName(2));
      } else {
         xSiteAdminOperations(0, 0, null).takeSiteOffline(siteName(1));
         xSiteAdminOperations(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name()).takeSiteOffline(siteName(1));
         xSiteAdminOperations(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name()).takeSiteOffline(siteName(2));
         xSiteAdminOperations(0, 0, CacheType.BACKUP_TO_SITE_2.name()).takeSiteOffline(siteName(2));
      }
   }

   private void putInAllCache(String key, String value) {
      cache(0, 0, null).put(key, value);
      cache(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name()).put(key, value);
      cache(0, 0, CacheType.BACKUP_TO_SITE_2.name()).put(key, value);
      cache(0, 0, CacheType.NO_BACKUP.name()).put(key, value);
   }

   private void assertValueInAllCachesInPrimarySite(String key, String value) {
      assertValueInCache(0, null, key, value);
      assertValueInCache(0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), key, value);
      assertValueInCache(0, CacheType.BACKUP_TO_SITE_2.name(), key, value);
      assertValueInCache(0, CacheType.NO_BACKUP.name(), key, value);
   }

   private void assertValueInCache(int siteIndex, String cacheName, String key, String value) {
      for (int nodeIndex = 0; nodeIndex < defaultNumberOfNodes(); nodeIndex++) {
         assertEquals(format("Wrong value for key '%s' in cache '%s' on site '%d' and node '%d'",
                             key, cacheName, siteIndex, nodeIndex),
                      value, cache(siteIndex, nodeIndex, cacheName).get(key));
      }
   }

   private XSiteAdminOperations xSiteAdminOperations(int siteIndex, int nodeIndex, String cacheName) {
      return TestingUtil.extractComponent(cache(siteIndex, nodeIndex, cacheName), XSiteAdminOperations.class);
   }

   private <K, V> Cache<K, V> cache(int siteIndex, int nodeIndex, String cacheName) {
      if (cacheName == null) {
         return site(siteIndex).cache(nodeIndex);
      } else {
         return site(siteIndex).cache(cacheName, nodeIndex);
      }
   }

   private void assertCacheEmpty(int siteIndex, int nodeIndex, String cacheName) {
      assertTrue(format("Cache '%s' is not empty in site '%d'", cacheName, siteIndex),
                 cache(siteIndex, nodeIndex, cacheName).isEmpty());
   }

   private void assertAllCachesEmpty() {
      for (CacheType cacheType : CacheType.values()) {
         assertCacheEmpty(0, 0, cacheType.name());
      }
      assertCacheEmpty(1, 0, null);
      assertCacheEmpty(1, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name());
      assertCacheEmpty(2, 0, CacheType.BACKUP_TO_SITE_2.name());
   }

   private void assertSiteStatusInAllCaches(String status) {
      assertSiteStatus(0, 0, null, 1, status);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 1, status);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_1_AND_2.name(), 2, status);
      assertSiteStatus(0, 0, CacheType.BACKUP_TO_SITE_2.name(), 2, status);
   }

   private void assertSiteStatus(int siteIndex, int nodeIndex, String cacheName, int backupSiteIndex, String status) {
      assertEquals(format("Wrong site status for cache '%s' in site '%d' for backup site '%d'.", cacheName, siteIndex, backupSiteIndex),
                   status,
                   xSiteAdminOperations(siteIndex, nodeIndex, cacheName).siteStatus(siteName(backupSiteIndex)));
   }

   private List<BlockingTransport> getBlockingTransport(int siteIndex, boolean createIfAbsent) {
      List<EmbeddedCacheManager> cacheManagerList = site(siteIndex).cacheManagers();
      List<BlockingTransport> blockingTransportList = new ArrayList<>(cacheManagerList.size());
      cacheManagerList.forEach(cacheManager -> {
         Transport transport = cacheManager.getTransport();
         if (transport instanceof BlockingTransport) {
            blockingTransportList.add((BlockingTransport) transport);
         } else if (createIfAbsent) {
            BlockingTransport blockingTransport = new BlockingTransport(transport);
            TestingUtil.replaceComponent(cacheManager, Transport.class, blockingTransport, true);
            blockingTransportList.add(blockingTransport);
         }
      });
      return blockingTransportList.isEmpty() ? Collections.emptyList() : blockingTransportList;
   }

   protected enum CacheType {
      BACKUP_TO_SITE_2,
      BACKUP_TO_SITE_1_AND_2,
      NO_BACKUP
   }

   private static class BlockingTransport extends AbstractDelegatingTransport {

      private final NotifierLatch notifierLatch;

      public BlockingTransport(Transport actual) {
         super(actual);
         notifierLatch = new NotifierLatch();
         notifierLatch.stopBlocking();
      }

      public void blockCommands() {
         notifierLatch.startBlocking();
      }

      public void unblockCommands() {
         notifierLatch.stopBlocking();
      }

      @Override
      public void start() {
         //skip start it again.
      }

      @Override
      protected void beforeBackupRemotely(XSiteReplicateCommand command) {
         if (command instanceof XSiteStatePushCommand) {
            notifierLatch.blockIfNeeded();
         }
      }
   }
}
