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
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "xsite.XSiteFileParsingTest")
public class XSiteFileParsing2Test extends SingleCacheManagerTest {

   public static final String FILE_NAME = "configs/xsite/xsite-test2.xml";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.fromXml(FILE_NAME);
      return embeddedCacheManager;
   }

   public void testDefaultCache() {
      Configuration dcc = cacheManager.getDefaultCacheConfiguration();
      assertEquals(dcc.sites().backups().size(), 2);
      testDefault(dcc);
   }

   public void testInheritor() {
      Configuration dcc = cacheManager.getCacheConfiguration("inheritor");
      testDefault(dcc);
   }

   public void testNoBackupFor() {
      Configuration dcc = cacheManager.getCacheConfiguration("noBackupFor");
      assertEquals(1, dcc.sites().backups().size());

      assertTrue(dcc.sites().backups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003, BackupFailurePolicy.WARN, null)));
      assertNull(dcc.sites().backupFor().remoteSite());
      assertNull(dcc.sites().backupFor().remoteCache());
   }
   public void testNoBackupFor2() {
      Configuration dcc = cacheManager.getCacheConfiguration("noBackupFor2");
      assertEquals(0, dcc.sites().backups().size());
      assertNull(dcc.sites().backupFor().remoteSite());
      assertNull(dcc.sites().backupFor().remoteCache());
   }

   private void testDefault(Configuration dcc) {
      assertTrue(dcc.sites().backups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003l, BackupFailurePolicy.IGNORE, null)));
      assertTrue(dcc.sites().backups().contains(new BackupConfiguration("SFO", BackupConfiguration.BackupStrategy.ASYNC,
                                                                        10000l, BackupFailurePolicy.WARN, null)));
      assertEquals("someCache", dcc.sites().backupFor().remoteCache());
      assertEquals("SFO", dcc.sites().backupFor().remoteSite());
   }

}
