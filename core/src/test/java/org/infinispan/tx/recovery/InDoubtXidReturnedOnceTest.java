/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import static org.infinispan.tx.recovery.RecoveryTestUtil.*;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.recovery.InDoubtXidReturnedOnceTest")
public class InDoubtXidReturnedOnceTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      configuration.fluent().locking().useLockStriping(false);
      configuration.fluent().transaction()
         .transactionManagerLookupClass(RecoveryDummyTransactionManagerLookup.class)
         .recovery();
      configuration.fluent().clustering().hash().rehashEnabled(false);
      configuration.fluent().hash().numOwners(3);
      configuration.fluent().transaction().recovery();

      createCluster(configuration, 4);
      waitForClusterToForm();
   }

   public void testXidReturnedOnlyOnce() throws Throwable {
      DummyTransaction dummyTransaction1 = beginAndSuspendTx(this.cache(3));
      prepareTransaction(dummyTransaction1);
      manager(3).stop();
      TestingUtil.blockUntilViewsReceived(60000, false, cache(0), cache(1), cache(2));
      TestingUtil.waitForRehashToComplete(cache(0), cache(1), cache(2));


      DummyTransaction dummyTransaction = beginAndSuspendTx(this.cache(0));
      Xid[] recover = dummyTransaction.firstEnlistedResource().recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
      assertEquals(recover.length,1);
      assertEquals(dummyTransaction1.getXid(), recover[0]);

   }
}
