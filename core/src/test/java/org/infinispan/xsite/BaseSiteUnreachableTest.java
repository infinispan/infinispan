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
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

@Test (groups = "xsite")
public abstract class BaseSiteUnreachableTest extends AbstractXSiteTest {

   protected BackupFailurePolicy lonBackupFailurePolicy = BackupFailurePolicy.WARN;
   protected BackupConfiguration.BackupStrategy lonBackupStrategy = BackupConfiguration.BackupStrategy.SYNC;
   protected String lonCustomFailurePolicyClass = null;
   protected int failures = 0;


   @Override
   protected void createSites() {

      GlobalConfigurationBuilder lonGc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      lonGc
            .site().localSite("LON");
      ConfigurationBuilder lon = getLonActiveConfig();
      lon.sites().addBackup()
               .site("NYC")
               .backupFailurePolicy(lonBackupFailurePolicy)
               .replicationTimeout(100) //keep it small so that the test doesn't take long to run
               .takeOffline().afterFailures(failures).
            backup()
               .strategy(lonBackupStrategy)
               .failurePolicyClass(lonCustomFailurePolicyClass);
      lon.sites().addInUseBackupSite("NYC");

      createSite("LON", 2, lonGc, lon);
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }
}
