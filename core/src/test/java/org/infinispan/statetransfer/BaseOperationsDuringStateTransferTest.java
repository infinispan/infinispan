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

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.CallInterceptor;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.VersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.topology.DefaultRebalancePolicy;
import org.infinispan.topology.RebalancePolicy;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional")
@CleanupAfterMethod
public abstract class BaseOperationsDuringStateTransferTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(BaseOperationsDuringStateTransferTest.class);

   private final CacheMode cacheMode;

   private final boolean isTransactional;

   private final boolean isOptimistic;

   private ConfigurationBuilder cacheConfigBuilder;

   private ReclosableLatch rebalanceGate;

   protected BaseOperationsDuringStateTransferTest(CacheMode cacheMode, boolean isTransactional, boolean isOptimistic) {
      this.cacheMode = cacheMode;
      this.isTransactional = isTransactional;
      this.isOptimistic = isOptimistic;
   }

   @Override
   protected void createCacheManagers() {
      cacheConfigBuilder = getDefaultClusteredCacheConfig(cacheMode, isTransactional, true);
      if (isTransactional) {
         cacheConfigBuilder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
               .transactionManagerLookup(new DummyTransactionManagerLookup())
               .syncCommitPhase(true).syncRollbackPhase(true);

         if (isOptimistic) {
            cacheConfigBuilder.transaction().lockingMode(LockingMode.OPTIMISTIC)
                  .locking().writeSkewCheck(true).isolationLevel(IsolationLevel.REPEATABLE_READ)
                  .versioning().enable().scheme(VersioningScheme.SIMPLE);
         } else {
            cacheConfigBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
         }
      }
      cacheConfigBuilder.clustering().hash().numSegments(10).numOwners(2)
            .l1().disable().onRehash(false)
            .locking().lockAcquisitionTimeout(1000l);
      cacheConfigBuilder.clustering().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false);

      rebalanceGate = new ReclosableLatch(true);

      addClusterEnabledCacheManager(cacheConfigBuilder);
      waitForClusterToForm();

      TestingUtil.replaceComponent(manager(0), RebalancePolicy.class,
            new DefaultRebalancePolicy() {
               @Override
               public void updateCacheStatus(String cacheName, ClusterCacheStatus cacheStatus) throws Exception {
                  if (cacheStatus.getCacheTopology().getPendingCH() != null) {
                     // block the rebalance until the test reaches the desired spot
                     try {
                        rebalanceGate.await();
                     } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                     }
                  }
                  super.updateCacheStatus(cacheName, cacheStatus);
               }
            }, true);
   }

   public void testRemove() throws Exception {
      cache(0).put("myKey", "myValue");

      // add an interceptor on second node that will block REMOVE commands right after EntryWrappingInterceptor until we are ready
      final CountDownLatch removeStartedLatch = new CountDownLatch(1);
      final CountDownLatch removeProceedLatch = new CountDownLatch(1);
      boolean isVersioningEnabled = cache(0).getCacheConfiguration().versioning().enabled();
      cacheConfigBuilder.customInterceptors().addInterceptor().after(isVersioningEnabled ? VersionedEntryWrappingInterceptor.class : EntryWrappingInterceptor.class).interceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            if (cmd instanceof RemoveCommand) {
               // signal we encounter a REMOVE
               removeStartedLatch.countDown();
               // wait until it is ok to continue with REMOVE
               if (!removeProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      });

      // do not allow coordinator to send topology updates to node B
      rebalanceGate.close();

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      log.info("Added a new node");

      // node B is not a member yet and rebalance has not started yet
      CacheTopology cacheTopology = advancedCache(1).getComponentRegistry().getStateTransferManager().getCacheTopology();
      assertNull(cacheTopology.getPendingCH());
      assertTrue(cacheTopology.getMembers().contains(address(0)));
      assertFalse(cacheTopology.getMembers().contains(address(1)));
      assertFalse(cacheTopology.getCurrentCH().getMembers().contains(address(1)));

      // no keys should be present on node B yet because state transfer is blocked
      assertTrue(cache(1).keySet().isEmpty());

      // initiate a REMOVE
      Future<Object> getFuture = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            try {
               return cache(1).remove("myKey");
            } catch (Exception e) {
               log.errorf(e, "PUT failed: %s", e.getMessage());
               throw e;
            }
         }
      });

      // wait for REMOVE command on node B to reach beyond *EntryWrappingInterceptor, where it will block.
      // the value seen so far is null
      if (!removeStartedLatch.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // paranoia, yes the value is still missing from data container
      assertTrue(cache(1).keySet().isEmpty());

      // allow rebalance to start
      rebalanceGate.open();

      // wait for state transfer to end
      TestingUtil.waitForRehashToComplete(cache(0), cache(1));

      // the state should be already transferred now
      assertEquals(1, cache(1).keySet().size());

      // allow REMOVE to continue
      removeProceedLatch.countDown();

      Object oldVal = getFuture.get(15, TimeUnit.SECONDS);
      assertNotNull(oldVal);
      assertEquals("myValue", oldVal);

      assertNull(cache(0).get("myKey"));
      assertNull(cache(1).get("myKey"));
   }

   public void testPut() throws Exception {
      cache(0).put("myKey", "myValue");

      // add an interceptor on second node that will block PUT commands right after EntryWrappingInterceptor until we are ready
      final CountDownLatch putStartedLatch = new CountDownLatch(1);
      final CountDownLatch putProceedLatch = new CountDownLatch(1);
      boolean isVersioningEnabled = cache(0).getCacheConfiguration().versioning().enabled();
      cacheConfigBuilder.customInterceptors().addInterceptor().after(isVersioningEnabled ? VersionedEntryWrappingInterceptor.class : EntryWrappingInterceptor.class).interceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            if (cmd instanceof PutKeyValueCommand && !((PutKeyValueCommand) cmd).hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
               // signal we encounter a (non-state-transfer) PUT
               putStartedLatch.countDown();
               // wait until it is ok to continue with PUT
               if (!putProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      });

      // do not allow coordinator to send topology updates to node B
      rebalanceGate.close();

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      log.info("Added a new node");

      // node B is not a member yet and rebalance has not started yet
      CacheTopology cacheTopology = advancedCache(1).getComponentRegistry().getStateTransferManager().getCacheTopology();
      assertNull(cacheTopology.getPendingCH());
      assertTrue(cacheTopology.getMembers().contains(address(0)));
      assertFalse(cacheTopology.getMembers().contains(address(1)));
      assertFalse(cacheTopology.getCurrentCH().getMembers().contains(address(1)));

      // no keys should be present on node B yet because state transfer is blocked
      assertTrue(cache(1).keySet().isEmpty());

      // initiate a PUT
      Future<Object> getFuture = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            try {
               return cache(1).put("myKey", "newValue");
            } catch (Exception e) {
               log.errorf(e, "PUT failed: %s", e.getMessage());
               throw e;
            }
         }
      });

      // wait for PUT command on node B to reach beyond *EntryWrappingInterceptor, where it will block.
      // the value seen so far is null
      if (!putStartedLatch.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // paranoia, yes the value is still missing from data container
      assertTrue(cache(1).keySet().isEmpty());

      // allow rebalance to start
      rebalanceGate.open();

      // wait for state transfer to end
      TestingUtil.waitForRehashToComplete(cache(0), cache(1));

      // the state should be already transferred now
      assertEquals(1, cache(1).keySet().size());

      // allow PUT to continue
      putProceedLatch.countDown();

      Object oldVal = getFuture.get(15, TimeUnit.SECONDS);
      assertNotNull(oldVal);
      assertEquals("myValue", oldVal);

      assertEquals("newValue", cache(0).get("myKey"));
      assertEquals("newValue", cache(1).get("myKey"));
   }

   public void testReplace() throws Exception {
      cache(0).put("myKey", "myValue");

      // add an interceptor on second node that will block REPLACE commands right after EntryWrappingInterceptor until we are ready
      final CountDownLatch replaceStartedLatch = new CountDownLatch(1);
      final CountDownLatch replaceProceedLatch = new CountDownLatch(1);
      boolean isVersioningEnabled = cache(0).getCacheConfiguration().versioning().enabled();
      cacheConfigBuilder.customInterceptors().addInterceptor().after(isVersioningEnabled ? VersionedEntryWrappingInterceptor.class : EntryWrappingInterceptor.class).interceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            if (cmd instanceof ReplaceCommand) {
               // signal we encounter a REPLACE
               replaceStartedLatch.countDown();
               // wait until it is ok to continue with REPLACE
               if (!replaceProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      });

      // do not allow coordinator to send topology updates to node B
      rebalanceGate.close();

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      log.info("Added a new node");

      // node B is not a member yet and rebalance has not started yet
      CacheTopology cacheTopology = advancedCache(1).getComponentRegistry().getStateTransferManager().getCacheTopology();
      assertNull(cacheTopology.getPendingCH());
      assertTrue(cacheTopology.getMembers().contains(address(0)));
      assertFalse(cacheTopology.getMembers().contains(address(1)));
      assertFalse(cacheTopology.getCurrentCH().getMembers().contains(address(1)));

      // no keys should be present on node B yet because state transfer is blocked
      assertTrue(cache(1).keySet().isEmpty());

      // initiate a REPLACE
      Future<Object> getFuture = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            try {
               return cache(1).replace("myKey", "newValue");
            } catch (Exception e) {
               log.errorf(e, "REPLACE failed: %s", e.getMessage());
               throw e;
            }
         }
      });

      // wait for REPLACE command on node B to reach beyond *EntryWrappingInterceptor, where it will block.
      // the value seen so far is null
      if (!replaceStartedLatch.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // paranoia, yes the value is still missing from data container
      assertTrue(cache(1).keySet().isEmpty());

      // allow rebalance to start
      rebalanceGate.open();

      // wait for state transfer to end
      TestingUtil.waitForRehashToComplete(cache(0), cache(1));

      // the state should be already transferred now
      assertEquals(1, cache(1).keySet().size());

      // allow REPLACE to continue
      replaceProceedLatch.countDown();

      Object oldVal = getFuture.get(15, TimeUnit.SECONDS);
      assertNotNull(oldVal);
      assertEquals("myValue", oldVal);

      assertEquals("newValue", cache(0).get("myKey"));
      assertEquals("newValue", cache(1).get("myKey"));
   }

   public void testGet() throws Exception {
      cache(0).put("myKey", "myValue");

      // add an interceptor on node B that will block state transfer until we are ready
      final CountDownLatch applyStateProceedLatch = new CountDownLatch(1);
      final CountDownLatch applyStateStartedLatch = new CountDownLatch(1);
      cacheConfigBuilder.customInterceptors().addInterceptor().before(InvocationContextInterceptor.class).interceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            // if this 'put' command is caused by state transfer we block until GET begins
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

      // add an interceptor on node B that will block GET commands until we are ready
      final CountDownLatch getKeyStartedLatch = new CountDownLatch(1);
      final CountDownLatch getKeyProceedLatch = new CountDownLatch(1);
      cacheConfigBuilder.customInterceptors().addInterceptor().before(CallInterceptor.class).interceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            if (cmd instanceof GetKeyValueCommand) {
               // signal we encounter a GET
               getKeyStartedLatch.countDown();
               // wait until it is ok to continue with GET
               if (!getKeyProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      });

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      log.info("Added a new node");

      // state transfer is blocked, no keys should be present on node B yet
      assertTrue(cache(1).keySet().isEmpty());

      // wait for state transfer on node B to progress to the point where data segments are about to be applied
      if (!applyStateStartedLatch.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // state transfer is blocked, no keys should be present on node B yet
      assertTrue(cache(1).keySet().isEmpty());

      // initiate a GET
      Future<Object> getFuture = fork(new Callable<Object>() {
         @Override
         public Object call() {
            return cache(1).get("myKey");
         }
      });

      // wait for GET command on node B to reach beyond *DistributionInterceptor, where it will block.
      // the value seen so far is null
      if (!getKeyStartedLatch.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // allow state transfer to apply state
      applyStateProceedLatch.countDown();

      // wait for state transfer to end
      TestingUtil.waitForRehashToComplete(cache(0), cache(1));

      assertEquals(1, cache(1).keySet().size());

      // allow GET to continue
      getKeyProceedLatch.countDown();

      Object value = getFuture.get(15, TimeUnit.SECONDS);
      assertEquals("myValue", value);
   }
}
