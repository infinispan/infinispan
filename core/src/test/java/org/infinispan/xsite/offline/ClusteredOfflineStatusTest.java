package org.infinispan.xsite.offline;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.annotations.Test;

@Test(groups = "xsite", testName = "xsite.offline.ClusteredOfflineStatusTest")
public class ClusteredOfflineStatusTest extends AbstractMultipleSitesTest {

   // it isn't important the site is not running.
   private static final String REMOTE_SITE_NAME = "_NYC_";

   @Override
   protected int defaultNumberOfNodes() {
      return 1;
   }

   @Override
   protected int defaultNumberOfSites() {
      return 1;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      var builder = super.defaultConfigurationForSite(siteIndex);
      builder.sites().addBackup().site(REMOTE_SITE_NAME);
      return builder;
   }

   public void testJoinerReceivesStatus() {
      var tom1 = takeOfflineManager(siteName(0), 0);

      tom1.takeSiteOffline(REMOTE_SITE_NAME);
      eventually(() -> tom1.getOfflineStatus(REMOTE_SITE_NAME).isOffline());

      // add a new member
      site(0).addCache(defaultGlobalConfigurationForSite(0), defaultConfigurationForSite(0));
      site(0).waitForClusterToForm(null);

      // check if it fetches the status
      var tom2 = takeOfflineManager(siteName(0), 1);
      eventually(() -> tom2.getOfflineStatus(REMOTE_SITE_NAME).isOffline());

      // bring online and check it is replicated
      tom2.bringSiteOnline(REMOTE_SITE_NAME);
      eventually(() -> !tom1.getOfflineStatus(REMOTE_SITE_NAME).isOffline());
      eventually(() -> !tom2.getOfflineStatus(REMOTE_SITE_NAME).isOffline());

      // take offline and check it is replicated
      tom2.takeSiteOffline(REMOTE_SITE_NAME);
      eventually(() -> tom1.getOfflineStatus(REMOTE_SITE_NAME).isOffline());
      eventually(() -> tom2.getOfflineStatus(REMOTE_SITE_NAME).isOffline());
   }
}
