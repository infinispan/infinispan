/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * Test for ISPN-2362 and ISPN-2502 in distributed mode. Uses a cluster which initially has 2 nodes
 * and then a third is added to test consistency of state transfer.
 * Tests several operations both in an optimistic tx cluster (with write-skew check enabled) and in a pessimistic tx one.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.DistStateTransferOnJoinConsistencyTest")
@CleanupAfterMethod
public class DistStateTransferOnJoinConsistencyTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(DistStateTransferOnJoinConsistencyTest.class);

   private enum Operation {
      REMOVE, CLEAR, PUT, PUT_MAP, PUT_IF_ABSENT, REPLACE
   }

   @Override
   protected final void createCacheManagers() {
      // cache managers will be created by each test
   }

   protected ConfigurationBuilder createConfigurationBuilder(boolean isOptimistic) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      builder.clustering().hash().numOwners(3).numSegments(2);
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
      builder.clustering().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false);
      return builder;
   }

   public void testRemoveOptimistic() throws Exception {
      testOperationDuringJoin(Operation.REMOVE, true);
   }

   public void testRemovePessimistic() throws Exception {
      testOperationDuringJoin(Operation.REMOVE, false);
   }

   public void testClearOptimistic() throws Exception {
      testOperationDuringJoin(Operation.CLEAR, true);
   }

   public void testClearPessimistic() throws Exception {
      testOperationDuringJoin(Operation.CLEAR, false);
   }

   public void testPutOptimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT, true);
   }

   public void testPutPessimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT, false);
   }

   public void testPutMapOptimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_MAP, true);
   }

   public void testPutMapPessimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_MAP, false);
   }

   public void testPutIfAbsentOptimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_IF_ABSENT, true);
   }

   public void testPutIfAbsentPessimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_IF_ABSENT, false);
   }

   public void testReplaceOptimistic() throws Exception {
      testOperationDuringJoin(Operation.REPLACE, true);
   }

   public void testReplacePessimistic() throws Exception {
      testOperationDuringJoin(Operation.REPLACE, false);
   }

   private void testOperationDuringJoin(Operation op, boolean isOptimistic) throws Exception {
      ConfigurationBuilder builder = createConfigurationBuilder(isOptimistic);

      createCluster(builder, 2);
      waitForClusterToForm();

      final int numKeys = 5;
      log.infof("Putting %d keys into cache ..", numKeys);
      for (int i = 0; i < numKeys; i++) {
         cache(0).put(i, "before_st_" + i);
      }
      log.info("Finished putting keys");

      for (int i = 0; i < numKeys; i++) {
         String expected = "before_st_" + i;
         assertValue(0, i, expected);
         assertValue(1, i, expected);
      }

      final CountDownLatch applyStateProceedLatch = new CountDownLatch(1);
      final CountDownLatch applyStateStartedLatch = new CountDownLatch(1);
      builder.customInterceptors().addInterceptor().before(InvocationContextInterceptor.class).interceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            // if this 'put' command is caused by state transfer we delay it to ensure other cache operations
            // are performed first and create opportunity for inconsistencies
            if (cmd instanceof PutKeyValueCommand && ((PutKeyValueCommand) cmd).hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
               // signal we encounter a state transfer PUT
               applyStateStartedLatch.countDown();
               // wait until it is ok to apply state
               if (!applyStateProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      });

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builder);
      log.info("Added a new node");

      DataContainer dc0 = advancedCache(0).getDataContainer();
      DataContainer dc1 = advancedCache(1).getDataContainer();
      DataContainer dc2 = advancedCache(2).getDataContainer();

      // wait for state transfer on node C to progress to the point where data segments are about to be applied
      if (!applyStateStartedLatch.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      if (op == Operation.CLEAR) {
         log.info("Clearing cache ..");
         cache(0).clear();
         log.info("Finished clearing cache");

         assertEquals(0, dc0.size());
         assertEquals(0, dc1.size());
      } else if (op == Operation.REMOVE) {
         log.info("Removing all keys one by one ..");
         for (int i = 0; i < numKeys; i++) {
            cache(0).remove(i);
         }
         log.info("Finished removing keys");

         assertEquals(0, dc0.size());
         assertEquals(0, dc1.size());
      } else if (op == Operation.PUT || op == Operation.PUT_MAP || op == Operation.REPLACE || op == Operation.PUT_IF_ABSENT) {
         log.info("Updating all keys ..");
         if (op == Operation.PUT) {
            for (int i = 0; i < numKeys; i++) {
               cache(0).put(i, "after_st_" + i);
            }
         } else if (op == Operation.PUT_MAP) {
            Map<Integer, String> toPut = new HashMap<Integer, String>();
            for (int i = 0; i < numKeys; i++) {
               toPut.put(i, "after_st_" + i);
            }
            cache(0).putAll(toPut);
         } else if (op == Operation.REPLACE) {
            for (int i = 0; i < numKeys; i++) {
               String expectedOldValue = "before_st_" + i;
               boolean replaced = cache(0).replace(i, expectedOldValue, "after_st_" + i);
               assertTrue(replaced);
            }
         } else { // PUT_IF_ABSENT
            for (int i = 0; i < numKeys; i++) {
               String expectedOldValue = "before_st_" + i;
               Object prevValue = cache(0).putIfAbsent(i, "after_st_" + i);
               assertEquals(expectedOldValue, prevValue);
            }
         }
         log.info("Finished updating keys");
      }

      // allow state transfer to apply state
      applyStateProceedLatch.countDown();

      // wait for apply state to end
      TestingUtil.waitForRehashToComplete(cache(0), cache(1), cache(2));

      // at this point state transfer is fully done
      log.infof("Data container of NodeA has %d keys: %s", dc0.size(), dc0.entrySet());
      log.infof("Data container of NodeB has %d keys: %s", dc1.size(), dc1.entrySet());
      log.infof("Data container of NodeC has %d keys: %s", dc2.size(), dc2.entrySet());

      if (op == Operation.CLEAR || op == Operation.REMOVE) {
         // caches should be empty. check that no keys were revived by an inconsistent state transfer
         for (int i = 0; i < numKeys; i++) {
            assertNull(dc0.get(i));
            assertNull(dc1.get(i));
            assertNull(dc2.get(i));
         }
      } else if (op == Operation.PUT || op == Operation.PUT_MAP || op == Operation.REPLACE) {
         // check that all values are the ones expected after state transfer and were not overwritten with old values carried by state transfer
         for (int i = 0; i < numKeys; i++) {
            String expectedValue = "after_st_" + i;
            assertValue(0, i, expectedValue);
            assertValue(1, i, expectedValue);
            assertValue(2, i, expectedValue);
         }
      } else { // PUT_IF_ABSENT
         // check that all values are the ones before state transfer
         for (int i = 0; i < numKeys; i++) {
            String expectedValue = "before_st_" + i;
            assertValue(0, i, expectedValue);
            assertValue(1, i, expectedValue);
            assertValue(2, i, expectedValue);
         }
      }
   }

   private void assertValue(int cacheIndex, int key, String expectedValue) {
      InternalCacheEntry ice = cache(cacheIndex).getAdvancedCache().getDataContainer().get(key);
      assertNotNull("Found null on cache " + cacheIndex, ice);
      assertEquals("Did not find the expected value on cache " + cacheIndex, expectedValue, ice.getValue());
      assertEquals("Did not find the expected value on cache " + cacheIndex, expectedValue, cache(cacheIndex).get(key));
   }
}
