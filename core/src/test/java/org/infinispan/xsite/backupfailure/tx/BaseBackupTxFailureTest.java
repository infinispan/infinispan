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

package org.infinispan.xsite.backupfailure.tx;

import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.backupfailure.BaseBackupFailureTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite")
public abstract class BaseBackupTxFailureTest extends AbstractTwoSitesTest {

   private BaseBackupFailureTest.FailureInterceptor failureInterceptor;

   protected BaseBackupTxFailureTest() {
      isLonBackupTransactional = true;
      lonBackupFailurePolicy = BackupFailurePolicy.FAIL;
      use2Pc = true;
   }

   @Override
   protected void createSites() {
      super.createSites();
      failureInterceptor = new BaseBackupFailureTest.FailureInterceptor();
      backup("LON").getAdvancedCache().addInterceptor(failureInterceptor, 1);
   }

   public void testPrepareFailure() {
      try {
         cache("LON", 0).put("k","v");
         fail("This should have thrown an exception");
      } catch (Exception e) {
      }
      assertNull(cache("LON",0).get("k"));
      assertNull(cache("LON",1).get("k"));
      assertNull(backup("LON").get("k"));
      assertEquals(0, txTable(cache("LON", 0)).getLocalTransactions().size());
      assertEquals(0, txTable(cache("LON", 1)).getLocalTransactions().size());
      assertEquals(0, txTable(backup("LON")).getLocalTransactions().size());
   }
}
