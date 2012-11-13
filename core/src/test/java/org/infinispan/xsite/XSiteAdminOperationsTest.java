/*
 * JBoss, Home of Professional Open Source
 *  Copyright 2012 Red Hat Inc. and/or its affiliates and other
 *  contributors as indicated by the @author tags. All rights reserved
 *  See the copyright.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  This is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as
 *  published by the Free Software Foundation; either version 2.1 of
 *  the License, or (at your option) any later version.
 *
 *  This software is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this software; if not, write to the Free
 *  Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 *  02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.xsite;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.admin.XSiteAdminOperationsTest")
public class XSiteAdminOperationsTest extends AbstractTwoSitesTest {

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   public void testSiteStatus() {
      assertEquals(admin("LON", 0).siteStatus("NYC"), XSiteAdminOperations.ONLINE);
      assertEquals(admin("LON", 1).siteStatus("NYC"), XSiteAdminOperations.ONLINE);

      assertEquals(XSiteAdminOperations.SUCCESS, admin("LON", 1).takeSiteOffline("NYC"));

      assertEquals(admin("LON", 0).siteStatus("NYC"), XSiteAdminOperations.OFFLINE);
      assertEquals(admin("LON", 1).siteStatus("NYC"), XSiteAdminOperations.OFFLINE);

      assertEquals(XSiteAdminOperations.SUCCESS, admin("LON", 1).bringSiteOnline("NYC"));
      assertEquals(admin("LON", 0).siteStatus("NYC"), XSiteAdminOperations.ONLINE);
      assertEquals(admin("LON", 1).siteStatus("NYC"), XSiteAdminOperations.ONLINE);
   }

   public void amendTakeOffline() {
      assertEquals(admin("LON", 0).siteStatus("NYC"), XSiteAdminOperations.ONLINE);
      assertEquals(admin("LON", 1).siteStatus("NYC"), XSiteAdminOperations.ONLINE);

      BackupSenderImpl bs = backupSender("LON", 0);
      OfflineStatus offlineStatus = bs.getOfflineStatus("NYC");
      assertEquals(offlineStatus.getTakeOffline(), new TakeOfflineConfiguration(0, 0));

      assertEquals(XSiteAdminOperations.SUCCESS, admin("LON", 1).amendTakeOffline("NYC", 7, 12));
      assertEquals(offlineStatus.getTakeOffline(), new TakeOfflineConfiguration(7, 12));

      assertEquals(XSiteAdminOperations.SUCCESS, admin("LON", 1).setTakeOfflineAfterFailures("NYC", 8));
      assertEquals(offlineStatus.getTakeOffline(), new TakeOfflineConfiguration(8, 12));

      assertEquals(XSiteAdminOperations.SUCCESS, admin("LON", 1).setTakeOfflineMinTimeToWait("NYC", 13));
      assertEquals(offlineStatus.getTakeOffline(), new TakeOfflineConfiguration(8, 13));

      assertEquals(admin("LON", 0).getTakeOfflineAfterFailures("NYC"), "8");
      assertEquals(admin("LON", 0).getTakeOfflineMinTimeToWait("NYC"), "13");
      assertEquals(admin("LON", 1).getTakeOfflineAfterFailures("NYC"), "8");
      assertEquals(admin("LON", 1).getTakeOfflineMinTimeToWait("NYC"), "13");
   }

   public void testStatus() {
      assertEquals(admin("LON", 0).status(), "NYC[ONLINE]");
      assertEquals(admin("LON", 1).status(), "NYC[ONLINE]");

      assertEquals(XSiteAdminOperations.SUCCESS, admin("LON", 1).takeSiteOffline("NYC"));

      assertEquals(admin("LON", 0).status(), "NYC[OFFLINE]");
      assertEquals(admin("LON", 1).status(), "NYC[OFFLINE]");

      assertEquals(XSiteAdminOperations.SUCCESS, admin("LON", 1).bringSiteOnline("NYC"));
      assertEquals(admin("LON", 0).status(), "NYC[ONLINE]");
      assertEquals(admin("LON", 1).status(), "NYC[ONLINE]");

   }

   private BackupSenderImpl backupSender(String site, int cache) {
      return (BackupSenderImpl) cache(site, cache).getAdvancedCache().getComponentRegistry().getComponent(BackupSender.class);
   }

   private XSiteAdminOperations admin(String site, int cache) {
      return cache(site, cache).getAdvancedCache().getComponentRegistry().getComponent(XSiteAdminOperations.class);
   }
}
