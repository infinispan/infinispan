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
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

/**
 * Base test class for ISPN-2362 and ISPN-2502 in distributed mode. Uses a cluster which initially has 3 nodes and
 * the second node is killed in order to cause a state transfer and test consistency.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional")
@CleanupAfterMethod
public abstract class BaseDistStateTransferConsistencyTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(BaseDistStateTransferConsistencyTest.class);

   private enum Operation {
      REMOVE, CLEAR, PUT, PUT_MAP, REPLACE
   }

   private final boolean isOptimistic;

   protected BaseDistStateTransferConsistencyTest(boolean isOptimistic) {
      this.isOptimistic = isOptimistic;
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
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

      builder.clustering().hash().numSegments(10).numOwners(2).l1().disable().onRehash(false).locking().lockAcquisitionTimeout(1000l);
      builder.clustering().stateTransfer().fetchInMemoryState(true).waitForInitialStateTransferToComplete(false);

      createCluster(builder, 3);
      waitForClusterToForm();
   }

   public void testRemove() throws Exception {
      testStateTransferConsistency(Operation.REMOVE);
   }

   public void testClear() throws Exception {
      testStateTransferConsistency(Operation.CLEAR);
   }

   public void testPut() throws Exception {
      testStateTransferConsistency(Operation.PUT);
   }

   public void testPutMap() throws Exception {
      testStateTransferConsistency(Operation.PUT_MAP);
   }

   @Test(enabled = false)  // disabled due to ISPN-2647
   public void testReplace() throws Exception {
      testStateTransferConsistency(Operation.REPLACE);
   }

   private void testStateTransferConsistency(Operation op) throws Exception {
      final int num = 5;
      log.infof("Putting %d keys into cache ..", num);
      for (int i = 0; i < num; i++) {
         cache(0).put(i, "before_st_" + i);
      }
      log.info("Finished putting keys");

      for (int i = 0; i < num; i++) {
         assertEquals("before_st_" + i, cache(0).get(i));
         assertEquals("before_st_" + i, cache(1).get(i));
         assertEquals("before_st_" + i, cache(2).get(i));
      }

      final CountDownLatch applyStateProceedLatch = new CountDownLatch(1);
      final CountDownLatch applyStateStartedLatch1 = new CountDownLatch(1);
      advancedCache(0).addInterceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            // if this 'put' command is caused by state transfer we delay it to ensure other cache operations
            // are performed first and create opportunity for inconsistencies
            if (cmd instanceof PutKeyValueCommand && ((PutKeyValueCommand) cmd).hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
               // signal we encounter a state transfer PUT
               applyStateStartedLatch1.countDown();
               // wait until it is ok to apply state
               if (!applyStateProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      }, 0);

      final CountDownLatch applyStateStartedLatch2 = new CountDownLatch(1);
      advancedCache(2).addInterceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            // if this 'put' command is caused by state transfer we delay it to ensure other cache operations
            // are performed first and create opportunity for inconsistencies
            if (cmd instanceof PutKeyValueCommand && ((PutKeyValueCommand) cmd).hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
               // signal we encounter a state transfer PUT
               applyStateStartedLatch2.countDown();
               // wait until it is ok to apply state
               if (!applyStateProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      }, 0);

      log.info("Killing node 1 ..");
      TestingUtil.killCacheManagers(manager(1));
      log.info("Node 1 killed");

      DataContainer dc0 = advancedCache(0).getDataContainer();
      DataContainer dc2 = advancedCache(2).getDataContainer();

      // wait for state transfer on nodes A and C to progress to the point where data segments are about to be applied
      if (!applyStateStartedLatch1.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }
      if (!applyStateStartedLatch2.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      if (op == Operation.CLEAR) {
         log.info("Clearing cache ..");
         cache(0).clear();
         log.info("Finished clearing cache");

         assertEquals(0, dc0.size());
         assertEquals(0, dc2.size());
      } else if (op == Operation.REMOVE) {
         log.info("Removing all keys one by one ..");
         for (int i = 0; i < num; i++) {
            cache(0).remove(i);
         }
         log.info("Finished removing keys");

         assertEquals(0, dc0.size());
         assertEquals(0, dc2.size());
      } else if (op == Operation.PUT || op == Operation.PUT_MAP || op == Operation.REPLACE) {
         log.info("Updating all keys ..");
         if (op == Operation.PUT) {
            for (int i = 0; i < num; i++) {
               cache(0).put(i, "after_st_" + i);
            }
         } else if (op == Operation.PUT_MAP) {
            Map<Integer, String> toPut = new HashMap<Integer, String>();
            for (int i = 0; i < num; i++) {
               toPut.put(i, "after_st_" + i);
            }
            cache(0).putAll(toPut);
         } else {
            for (int i = 0; i < num; i++) {
               String expectedOldValue = "before_st_" + i;
               boolean replaced = cache(0).replace(i, expectedOldValue, "after_st_" + i);
               assertTrue(replaced);
            }
         }
         log.info("Finished updating keys");
      }

      // allow state transfer to apply state
      applyStateProceedLatch.countDown();

      // wait for apply state to end
      TestingUtil.waitForRehashToComplete(cache(0), cache(2));

      // at this point state transfer is fully done
      log.infof("Data container of NodeA has %d keys: %s", dc0.size(), dc0.keySet());
      log.infof("Data container of NodeC has %d keys: %s", dc2.size(), dc2.keySet());

      if (op == Operation.CLEAR || op == Operation.REMOVE) {
         // caches should be empty. check that no keys were revived by an inconsistent state transfer
         for (int i = 0; i < num; i++) {
            assertNull(dc0.get(i));
            assertNull(dc2.get(i));
         }
      } else if (op == Operation.PUT || op == Operation.PUT_MAP || op == Operation.REPLACE) {
         ConsistentHash ch = advancedCache(0).getComponentRegistry().getStateTransferManager().getCacheTopology().getReadConsistentHash();
         // check that all values are the ones expected after state transfer
         for (int i = 0; i < num; i++) {
            // check values were not overwritten with old values carried by state transfer
            assertEquals("after_st_" + i, cache(0).get(i));
            assertEquals("after_st_" + i, cache(2).get(i));

            // check number of owners
            int owners = 0;
            if (dc0.get(i) != null) {
               owners++;
            }
            if (dc2.get(i) != null) {
               owners++;
            }
            assertEquals("Wrong number of owners", ch.locateOwners(i).size(), owners);
         }
      }
   }
}
