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

package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.commitTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 */
@Test(groups = "functional", testName = "tx.ParticipantFailsAfterPrepareTest")
public class ParticipantFailsAfterPrepareTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      configuration.fluent().locking().useLockStriping(false);
      configuration.fluent().transaction()
         .transactionManagerLookupClass(DummyTransactionManagerLookup.class);
      configuration.fluent().clustering().hash().rehashEnabled(false);
      configuration.fluent().clustering().hash().numOwners(3);
      createCluster(configuration, 4);
      waitForClusterToForm();
   }

   public void testNonOriginatorFailsAfterPrepare() throws Exception {
      DummyTransaction dummyTransaction = beginAndSuspendTx(cache(0), getKeyForCache(0));
      prepareTransaction(dummyTransaction);

      int indexToKill = -1;
      //this tx spreads over 3 out of 4 nodes, let's find one that has the tx and kill it
      for (int i = 3; i > 0; i--) {
         if (lockManager(i).getNumberOfLocksHeld() == 1) {
            indexToKill = i;
            break;
         }
      }

      assert indexToKill > 0;
      System.out.println("indexToKill = " + indexToKill);

      Address toKill = address(indexToKill);
      TestingUtil.killCacheManagers(manager(indexToKill));

      List<Cache> participants;
      participants = getAliveParticipants(indexToKill);

      TestingUtil.blockUntilViewsReceived(60000, false, participants);

      //one of the participants must not have a prepare on it
      boolean noLocks = false;
      for (Cache c : participants) {
         if (TestingUtil.extractLockManager(c).getNumberOfLocksHeld() == 0) noLocks = true;
      }
      assert noLocks;

      log.trace("About to commit. Killed node is: " + toKill);

      try {
         commitTransaction(dummyTransaction);
      } catch (Throwable t) {
         t.printStackTrace();
         assert false; //this should not have failed
      } finally {
         //now check weather all caches have the same content and no locks acquired
         for (Cache c : participants) {
            assertEquals(TestingUtil.extractLockManager(c).getNumberOfLocksHeld(), 0);
            assertEquals(c.keySet().size(), 1);
         }
      }
   }

   private List<Cache> getAliveParticipants(int indexToKill) {
      List<Cache> participants = new ArrayList<Cache>();
      for (int i = 0; i < 4; i++) {
         if (i == indexToKill) continue;
         participants.add(cache(i));
      }
      return participants;
   }
}