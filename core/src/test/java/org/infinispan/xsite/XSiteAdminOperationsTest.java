package org.infinispan.xsite;

import static java.lang.String.format;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.TakeOfflineConfigurationBuilder;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.testing.Exceptions;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.XSiteAdminOperationsTest")
public class XSiteAdminOperationsTest extends AbstractTwoSitesTest {

   public XSiteAdminOperationsTest() {
      //async LON=>NYC, sync NYC=>LON
      //the above doesn't make sense, and probably won't work, but we don't put any data
      //we are just testing XSiteAdminOperations class.
      this.lonBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   public void testSiteStatus() {
      assertEquals(XSiteAdminOperations.ONLINE, admin(LON, 0).siteStatus(NYC));
      assertEquals(XSiteAdminOperations.ONLINE, admin(LON, 1).siteStatus(NYC));

      assertEquals(XSiteAdminOperations.SUCCESS, admin(LON, 1).takeSiteOffline(NYC));

      assertEquals(XSiteAdminOperations.OFFLINE, admin(LON, 0).siteStatus(NYC));
      assertEquals(XSiteAdminOperations.OFFLINE, admin(LON, 1).siteStatus(NYC));

      assertEquals(XSiteAdminOperations.SUCCESS, admin(LON, 1).bringSiteOnline(NYC));
      assertEquals(XSiteAdminOperations.ONLINE, admin(LON, 0).siteStatus(NYC));
      assertEquals(XSiteAdminOperations.ONLINE, admin(LON, 1).siteStatus(NYC));
   }

   public void amendTakeOffline() {
      assertEquals(XSiteAdminOperations.ONLINE, admin(LON, 0).siteStatus(NYC));
      assertEquals(XSiteAdminOperations.ONLINE, admin(LON, 1).siteStatus(NYC));

      TakeOfflineManager tom = takeOfflineManager(LON, 0);
      assertEquals(tom.getConfiguration(NYC), new TakeOfflineConfigurationBuilder(null, null).afterFailures(0).minTimeToWait(0).create());

      assertEquals(XSiteAdminOperations.SUCCESS, admin(LON, 1).amendTakeOffline(NYC, 7, 12));
      assertEquals(tom.getConfiguration(NYC), new TakeOfflineConfigurationBuilder(null, null).afterFailures(7).minTimeToWait(12).create());

      assertEquals(XSiteAdminOperations.SUCCESS, admin(LON, 1).setTakeOfflineAfterFailures(NYC, 8));
      assertEquals(tom.getConfiguration(NYC), new TakeOfflineConfigurationBuilder(null, null).afterFailures(8).minTimeToWait(12).create());

      assertEquals(XSiteAdminOperations.SUCCESS, admin(LON, 1).setTakeOfflineMinTimeToWait(NYC, 13));
      assertEquals(tom.getConfiguration(NYC), new TakeOfflineConfigurationBuilder(null, null).afterFailures(8).minTimeToWait(13).create());

      assertEquals("8", admin(LON, 0).getTakeOfflineAfterFailures(NYC));
      assertEquals("13", admin(LON, 0).getTakeOfflineMinTimeToWait(NYC));
      assertEquals("8", admin(LON, 1).getTakeOfflineAfterFailures(NYC));
      assertEquals("13", admin(LON, 1).getTakeOfflineMinTimeToWait(NYC));
   }

   public void testStatus() {
      assertEquals(admin(LON, 0).status(), format("%s[ONLINE]", NYC));
      assertEquals(admin(LON, 1).status(), format("%s[ONLINE]", NYC));

      assertEquals(XSiteAdminOperations.SUCCESS, admin(LON, 1).takeSiteOffline(NYC));

      assertEquals(admin(LON, 0).status(), format("%s[OFFLINE]", NYC));
      assertEquals(admin(LON, 1).status(), format("%s[OFFLINE]", NYC));

      assertEquals(XSiteAdminOperations.SUCCESS, admin(LON, 1).bringSiteOnline(NYC));
      assertEquals(admin(LON, 0).status(), format("%s[ONLINE]", NYC));
      assertEquals(admin(LON, 1).status(), format("%s[ONLINE]", NYC));
   }

   public void testStateTransferMode() {
      for (int i = 0; i < initialClusterSize; ++i) {
         //by default, it is manual
         assertEquals(XSiteStateTransferMode.MANUAL.toString(), admin(LON, i).getStateTransferMode(NYC));
         assertEquals(XSiteStateTransferMode.MANUAL.toString(), admin(NYC, i).getStateTransferMode(LON));
      }

      assertTrue(admin(LON, 0).setStateTransferMode(NYC, XSiteStateTransferMode.AUTO.toString()));

      for (int i = 0; i < initialClusterSize; ++i) {
         assertEquals(XSiteStateTransferMode.AUTO.toString(), admin(LON, i).getStateTransferMode(NYC));
         assertEquals(XSiteStateTransferMode.MANUAL.toString(), admin(NYC, i).getStateTransferMode(LON));
      }

      //sync mode throws an exception
      Exceptions.expectException(CacheConfigurationException.class,
            "ISPN000634.*",
            () -> admin(NYC, 0).setStateTransferMode(LON, XSiteStateTransferMode.AUTO.toString()));

      for (int i = 0; i < initialClusterSize; ++i) {
         assertEquals(XSiteStateTransferMode.AUTO.toString(), admin(LON, i).getStateTransferMode(NYC));
         assertEquals(XSiteStateTransferMode.MANUAL.toString(), admin(NYC, i).getStateTransferMode(LON));
      }

      assertTrue(admin(LON, 0).setStateTransferMode(NYC, XSiteStateTransferMode.MANUAL.toString()));

      for (int i = 0; i < initialClusterSize; ++i) {
         assertEquals(XSiteStateTransferMode.MANUAL.toString(), admin(LON, i).getStateTransferMode(NYC));
         assertEquals(XSiteStateTransferMode.MANUAL.toString(), admin(NYC, i).getStateTransferMode(LON));
      }

      // NYC already in manual mode, should return "false"
      assertFalse(admin(LON, 0).setStateTransferMode(NYC, XSiteStateTransferMode.MANUAL.toString()));
   }

   public void testSitesView() {
      assertEquals(new HashSet<>(Arrays.asList(LON, NYC)),
            extractGlobalComponent(site(LON).cacheManagers().get(0), Transport.class).getSitesView());
      assertEquals(new HashSet<>(Arrays.asList(LON, NYC)),
            extractGlobalComponent(site(LON).cacheManagers().get(0), Transport.class).getSitesView());
   }

   private XSiteAdminOperations admin(String site, int cache) {
      return extractComponent(cache(site, cache), XSiteAdminOperations.class);
   }
}
