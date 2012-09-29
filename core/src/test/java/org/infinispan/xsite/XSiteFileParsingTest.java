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
import org.infinispan.configuration.cache.BackupForConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "xsite.XSiteFileParsingTest")
public class XSiteFileParsingTest extends SingleCacheManagerTest {

   public static final String FILE_NAME = "configs/xsite/xsite-test.xml";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.fromXml(FILE_NAME);
      return embeddedCacheManager;
   }

   public void testGlobalConfiguration() {
      GlobalConfiguration cmc = cacheManager.getCacheManagerConfiguration();
      assertEquals("LON", cmc.sites().localSite());
   }

   public void testDefaultCache() {
      Configuration dcc = cacheManager.getDefaultCacheConfiguration();
      testDefault(dcc);
   }

   public void testBackupNyc() {
      Configuration dcc = cacheManager.getCacheConfiguration("backupNyc");
      assertEquals(dcc.sites().allBackups().size(), 0);
      BackupForConfiguration backupForConfiguration = dcc.sites().backupFor();
      assertEquals("someCache", backupForConfiguration.remoteCache());
      assertEquals("NYC", backupForConfiguration.remoteSite());
   }

   public void testInheritor() {
      Configuration dcc = cacheManager.getCacheConfiguration("inheritor");
      testDefault(dcc);
   }

   public void testNoBackups() {
      Configuration dcc = cacheManager.getCacheConfiguration("noBackups");
      assertEquals(dcc.sites().allBackups().size(), 0);
      assertEquals(dcc.sites().backupFor().remoteCache(), null);
      assertEquals(dcc.sites().backupFor().remoteSite(), null);
   }

   public void testCustomBackupPolicy() {
      Configuration dcc = cacheManager.getCacheConfiguration("customBackupPolicy");
      assertEquals(dcc.sites().allBackups().size(), 1);

            assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC2", BackupConfiguration.BackupStrategy.SYNC,
                                                                              160000, BackupFailurePolicy.CUSTOM,
                                                                              CountingCustomFailurePolicy.class.getName(),
                                                                              new TakeOfflineConfiguration(0, 0))));
      assertEquals(dcc.sites().backupFor().remoteCache(), null);
   }

   private void testDefault(Configuration dcc) {
      assertEquals(dcc.sites().allBackups().size(), 2);
      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003l, BackupFailurePolicy.IGNORE, null, new TakeOfflineConfiguration(0, 0))));
      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("SFO", BackupConfiguration.BackupStrategy.ASYNC,
                                                                        10000l, BackupFailurePolicy.WARN, null, new TakeOfflineConfiguration(0, 0))));
   }
}
