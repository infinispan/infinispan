/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.tx.totalorder.writeskew;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.versioning.DistL1WriteSkewTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

import static org.testng.Assert.assertEquals;

/**
 * A simple write skew check test for total order based protocol in distributed mode and with L1 cache enabled
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.DistTotalOrderL1WriteSkewTest")
@CleanupAfterMethod
public class DistTotalOrderL1WriteSkewTest extends DistL1WriteSkewTest {
   public void transactionCleanupWithWriteSkew() throws Exception {
      cache(0).put("k", "v");
      tm(0).begin();
      assertEquals("v", cache(0).get("k"));
      cache(0).put("k", "v2");
      Transaction suspend = tm(0).suspend();

      cache(0).put("k", "v3");
      tm(0).resume(suspend);
      try {
         tm(0).commit();
         assert false;
      } catch (Throwable e) {
         //expected
         e.printStackTrace();
      }
      assertEquals("v3", cache(0).get("k"));
      assertEventuallyEquals(1, "k", "v3");
      assertNoTransactions();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .recovery().disable();
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);
   }
}
