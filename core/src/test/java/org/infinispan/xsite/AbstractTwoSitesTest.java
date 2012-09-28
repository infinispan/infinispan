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

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;

import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public abstract class AbstractTwoSitesTest extends AbstractXSiteTest {

   protected BackupFailurePolicy lonBackupFailurePolicy = BackupFailurePolicy.WARN;
   protected boolean isLonBackupTransactional = false;
   protected BackupConfiguration.BackupStrategy lonBackupStrategy = BackupConfiguration.BackupStrategy.SYNC;
   protected String lonCustomFailurePolicyClass = null;

   @Override
   protected void createSites() {

      GlobalConfigurationBuilder lonGc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      lonGc
            .sites().localSite("LON");
      ConfigurationBuilder lon = getLonActiveConfig();
      lon.sites().addBackup()
            .site("NYC")
            .backupFailurePolicy(lonBackupFailurePolicy)
            .strategy(lonBackupStrategy)
            .failurePolicyClass(lonCustomFailurePolicyClass)
            .sites().addInUseBackupSite("NYC");
      ConfigurationBuilder nycBackup = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      nycBackup.sites().backupFor().remoteSite("NYC").defaultRemoteCache();

      GlobalConfigurationBuilder nycGc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      nycGc
            .sites().localSite("NYC");
      ConfigurationBuilder nyc = getNycActiveConfig();
      nyc.sites().addBackup()
            .site("LON")
            .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addInUseBackupSite("LON");
      ConfigurationBuilder lonBackup = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, isLonBackupTransactional);
      lonBackup.sites().backupFor().remoteSite("LON").defaultRemoteCache();


      createSite("LON", 2, lonGc, lon);
      createSite("NYC", 2, nycGc, nyc);

      startCache("LON", "nycBackup", nycBackup);
      startCache("NYC", "lonBackup", lonBackup);

      Configuration lonBackupConfig = cache("NYC", "lonBackup", 0).getCacheConfiguration();
      assertTrue(lonBackupConfig.sites().backupFor().isBackupFor("LON", CacheContainer.DEFAULT_CACHE_NAME));
   }

   protected Cache<Object, Object> backup(String site) {
      if (site.equals("LON")) return cache("NYC", "lonBackup", 0);
      if (site.equals("NYC")) return cache("LON", "nycBackup", 0);
      throw new IllegalArgumentException("No such site: " + site);
   }

   protected String val(String site) {
      return "v_" + site;
   }

   protected String key(String site) {
      return "k_" + site;
   }


   protected abstract ConfigurationBuilder getNycActiveConfig();

   protected abstract ConfigurationBuilder getLonActiveConfig();
}
