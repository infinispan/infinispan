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

package org.infinispan.xsite.offline;

import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.BackupSenderImpl;
import org.infinispan.xsite.BaseSiteUnreachableTest;
import org.infinispan.xsite.OfflineStatus;
import org.junit.Assert;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.offline.NonTxOfflineTest")
public class NonTxOfflineTest extends BaseSiteUnreachableTest {

   private static final int FAILURES = 8;
   protected int nrRpcPerPut = 1;

   public NonTxOfflineTest() {
      failures = FAILURES;
      lonBackupFailurePolicy = BackupFailurePolicy.FAIL;
   }

   public void testPutWithFailures() {

      BackupSenderImpl bs = (BackupSenderImpl) cache("LON", 0).getAdvancedCache().getComponentRegistry().getComponent(BackupSender.class);
      OfflineStatus nycStatus = bs.getOfflineStatus("NYC");

      for (int i = 0; i < FAILURES / nrRpcPerPut; i++) {
         try {
            assertEquals(BackupSender.BringSiteOnlineResponse.ALREADY_ONLINE, bs.bringSiteOnline("NYC"));
            cache("LON", 0).put("k" + i, "v" + i);
            fail("This should have failed");
         } catch (Exception e) {
            Assert.assertEquals(i + 1, nycStatus.getFailureCount());
         }
      }

      assert nycStatus.isOffline();

      for (int i = 0; i < FAILURES; i++) {
         cache("LON", 0).put("k" + i, "v" + i);
      }

      for (int i = 0; i < FAILURES; i++) {
         assertEquals("v" + i, cache("LON", 0).get("k" + i));
      }

      assertEquals(BackupSender.BringSiteOnlineResponse.NO_SUCH_SITE, bs.bringSiteOnline("NO_SITE"));

      assertEquals(BackupSender.BringSiteOnlineResponse.BROUGHT_ONLINE, bs.bringSiteOnline("NYC"));

      for (int i = 0; i < FAILURES / nrRpcPerPut; i++) {
         try {
            cache("LON", 0).put("k" + i, "v" + i);
            fail("This should have failed");
         } catch (Exception e) {
            //expected
         }
      }
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }
}
