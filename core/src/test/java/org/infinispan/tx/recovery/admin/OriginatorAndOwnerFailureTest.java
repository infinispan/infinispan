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

package org.infinispan.tx.recovery.admin;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.testng.Assert.assertEquals;

/**
 * Tests the following scenario: the transaction originator fails and it also part of the transactions.
 *
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.admin.OriginatorAndOwnerFailureTest")
@CleanupAfterMethod
public class OriginatorAndOwnerFailureTest extends AbstractRecoveryTest {

   private Object key;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = defaultRecoveryConfig();
      assert configuration.build().transaction().transactionMode().isTransactional();
      createCluster(configuration, 3);
      waitForClusterToForm();

      key = getKey();

      tm(2).begin();
      cache(2).put(this.key, "newValue");
      DummyTransaction tx = (DummyTransaction) tm(2).suspend();
      prepareTransaction(tx);

      killMember(2);

      assert !recoveryOps(0).showInDoubtTransactions().isEmpty();
      assert !recoveryOps(1).showInDoubtTransactions().isEmpty();
   }

   protected Object getKey() {
      return new MagicKey(cache(2));
   }

   public void recoveryInvokedOnNonTxParticipantTest() {
      runTest(false);
   }

   public void recoveryInvokedOnTxParticipantTest() {
      runTest(true);
   }

   private void runTest(boolean txParticipant) {
      int index = getTxParticipant(txParticipant);
      runTest(index);
   }

   protected void runTest(int index) {

      assert cache(0).getConfiguration().isTransactionalCache();

      List<Long> internalIds = getInternalIds(recoveryOps(index).showInDoubtTransactions());
      assertEquals(internalIds.size(), 1);

      assertEquals(cache(0).get(key), null);
      assertEquals(cache(1).get(key), null);

      log.trace("About to force commit!");
      isSuccess(recoveryOps(index).forceCommit(internalIds.get(0)));

      assertEquals(cache(0).get(key), "newValue");
      assertEquals(cache(1).get(key), "newValue");

      assertCleanup(0);
      assertCleanup(1);
   }
}
