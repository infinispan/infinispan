/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Test for ISPN-2362 and ISPN-2502 in replicated mode. Uses a cluster which initially has 2 nodes
 * and then a third is added to test consistency of state transfer.
 * Tests several operations both in an optimistic tx cluster (with write-skew check enabled) and in a pessimistic tx one.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.ReplStateTransferOnJoinConsistencyTest")
@CleanupAfterMethod
public class ReplStateTransferOnJoinConsistencyTest extends DistStateTransferOnJoinConsistencyTest {

   @Override
   protected ConfigurationBuilder createConfigurationBuilder(boolean isOptimistic) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true, true);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .syncCommitPhase(true).syncRollbackPhase(true);

      if (isOptimistic) {
         builder.transaction().lockingMode(LockingMode.OPTIMISTIC)
               .locking().writeSkewCheck(true).isolationLevel(IsolationLevel.REPEATABLE_READ)
               .versioning().enable().scheme(VersioningScheme.SIMPLE);
      } else {
         builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      }

      builder.clustering().l1().disable().onRehash(false).locking().lockAcquisitionTimeout(1000l);
      builder.clustering().hash().numSegments(10)
            .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false);
      return builder;
   }
}