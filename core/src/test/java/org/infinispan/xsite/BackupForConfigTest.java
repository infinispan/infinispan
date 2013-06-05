/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.xsite;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.BackupForConfigTest")
public class BackupForConfigTest extends SingleCacheManagerTest {

   ConfigurationBuilder nycBackup;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder lonGc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      lonGc.site().localSite("LON");
      ConfigurationBuilder lon = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      lon.sites().addBackup()
            .site("NYC")
            .strategy(BackupConfiguration.BackupStrategy.SYNC);
      nycBackup = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      nycBackup.sites().backupFor().remoteSite("NYC").defaultRemoteCache();

      // Remember to not do nothing else other than
      // creating the cache manager in order to avoid leaks
      return TestCacheManagerFactory.createClusteredCacheManager(lonGc, lon);
   }

   public void testBackupForIsCorrect() {
      cacheManager.getCache(); //start default cache
      cacheManager.defineConfiguration("nycBackup", nycBackup.build());
      cacheManager.getCache("nycBackup");
      SitesConfiguration sitesConfig = cache("nycBackup").getCacheConfiguration().sites();
      assertEquals(CacheContainer.DEFAULT_CACHE_NAME, sitesConfig.backupFor().remoteCache());
      assertEquals("NYC", sitesConfig.backupFor().remoteSite());
      sitesConfig.backupFor().isBackupFor("NYC", CacheContainer.DEFAULT_CACHE_NAME);
   }
}
