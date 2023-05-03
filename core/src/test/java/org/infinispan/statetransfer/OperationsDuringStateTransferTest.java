package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.interceptors.impl.VersionedEntryWrappingInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.OperationsDuringStateTransferTest")
@CleanupAfterMethod
public class OperationsDuringStateTransferTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(OperationsDuringStateTransferTest.class);

   private ConfigurationBuilder cacheConfigBuilder;

   @Override
   public Object[] factory() {
      return new Object[] {
         new OperationsDuringStateTransferTest().cacheMode(CacheMode.DIST_SYNC).transactional(false),
         new OperationsDuringStateTransferTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
         new OperationsDuringStateTransferTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC),
         new OperationsDuringStateTransferTest().cacheMode(CacheMode.REPL_SYNC).transactional(false),
         new OperationsDuringStateTransferTest().cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
         new OperationsDuringStateTransferTest().cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC),
      };
   }

   @Override
   protected void createCacheManagers() {
      cacheConfigBuilder = getDefaultClusteredCacheConfig(cacheMode, transactional, true);
      if (transactional) {
         cacheConfigBuilder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
               .transactionManagerLookup(new EmbeddedTransactionManagerLookup());

         cacheConfigBuilder.transaction().lockingMode(lockingMode);
         if (lockingMode == LockingMode.OPTIMISTIC) {
            cacheConfigBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
         }
      }
      cacheConfigBuilder.clustering().hash().numSegments(10)
            .l1().disable()
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      cacheConfigBuilder.clustering().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false);

      addClusterEnabledCacheManager(cacheConfigBuilder);
      waitForClusterToForm();
   }

   public void testRemove() throws Exception {
      cache(0).put("myKey", "myValue");

      // add an interceptor on second node that will block REMOVE commands right after EntryWrappingInterceptor until we are ready
      final CountDownLatch removeStartedLatch = new CountDownLatch(1);
      final CountDownLatch removeProceedLatch = new CountDownLatch(1);
      cacheConfigBuilder.customInterceptors().addInterceptor().after(ewi())
                        .interceptor(new RemoveLatchInterceptor(removeStartedLatch, removeProceedLatch));

      // do not allow coordinator to send topology updates to node B
      final ClusterTopologyManager ctm0 = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      ctm0.setRebalancingEnabled(false);

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      log.info("Added a new node");

      // node B is not a member yet and rebalance has not started yet
      CacheTopology cacheTopology = advancedCache(1).getDistributionManager().getCacheTopology();
      assertNull(cacheTopology.getPendingCH());
      assertTrue(cacheTopology.getMembers().contains(address(0)));
      assertFalse(cacheTopology.getMembers().contains(address(1)));
      assertFalse(cacheTopology.getCurrentCH().getMembers().contains(address(1)));

      // no keys should be present on node B yet because state transfer is blocked
      assertTrue(cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().isEmpty());

      // initiate a REMOVE
      Future<Object> getFuture = fork(() -> {
         try {
            return cache(1).remove("myKey");
         } catch (Exception e) {
            log.errorf(e, "PUT failed: %s", e.getMessage());
            throw e;
         }
      });

      // wait for REMOVE command on node B to reach beyond *EntryWrappingInterceptor, where it will block.
      // the value seen so far is null
      if (!removeStartedLatch.await(10, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // paranoia, yes the value is still missing from data container
      assertTrue(cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().isEmpty());

      // allow rebalance to start
      ctm0.setRebalancingEnabled(true);

      // wait for state transfer to end
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      // the state should be already transferred now
      assertEquals(1, cache(1).keySet().size());

      // allow REMOVE to continue
      removeProceedLatch.countDown();

      Object oldVal = getFuture.get(10, TimeUnit.SECONDS);
      assertNotNull(oldVal);
      assertEquals("myValue", oldVal);

      assertNull(cache(0).get("myKey"));
      assertNull(cache(1).get("myKey"));
   }

   public Class<? extends DDAsyncInterceptor> ewi() {
      Class<? extends DDAsyncInterceptor> after;
      if (Configurations.isTxVersioned(cache(0).getCacheConfiguration())) {
         after = VersionedEntryWrappingInterceptor.class;
      } else {
         after = EntryWrappingInterceptor.class;
      }
      return after;
   }

   public void testPut() throws Exception {
      cache(0).put("myKey", "myValue");

      // add an interceptor on second node that will block PUT commands right after EntryWrappingInterceptor until we are ready
      final CountDownLatch putStartedLatch = new CountDownLatch(1);
      final CountDownLatch putProceedLatch = new CountDownLatch(1);
      cacheConfigBuilder.customInterceptors().addInterceptor().after(ewi())
                        .interceptor(new PutLatchInterceptor(putStartedLatch, putProceedLatch));

      // do not allow coordinator to send topology updates to node B
      final ClusterTopologyManager ctm0 = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      ctm0.setRebalancingEnabled(false);

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      log.info("Added a new node");

      // node B is not a member yet and rebalance has not started yet
      CacheTopology cacheTopology = advancedCache(1).getDistributionManager().getCacheTopology();
      assertNull(cacheTopology.getPendingCH());
      assertTrue(cacheTopology.getMembers().contains(address(0)));
      assertFalse(cacheTopology.getMembers().contains(address(1)));
      assertFalse(cacheTopology.getCurrentCH().getMembers().contains(address(1)));

      // no keys should be present on node B yet because state transfer is blocked
      assertTrue(cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().isEmpty());

      // initiate a PUT
      Future<Object> putFuture = fork(() -> {
         try {
            return cache(1).put("myKey", "newValue");
         } catch (Exception e) {
            log.errorf(e, "PUT failed: %s", e.getMessage());
            throw e;
         }
      });

      // wait for PUT command on node B to reach beyond *EntryWrappingInterceptor, where it will block.
      // the value seen so far is null
      if (!putStartedLatch.await(10, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // paranoia, yes the value is still missing from data container
      assertTrue(cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().isEmpty());

      // allow rebalance to start
      ctm0.setRebalancingEnabled(true);

      // wait for state transfer to end
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      // the state should be already transferred now
      assertEquals(1, cache(1).keySet().size());

      // allow PUT to continue
      putProceedLatch.countDown();

      Object oldVal = putFuture.get(10, TimeUnit.SECONDS);
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
      cacheConfigBuilder.customInterceptors().addInterceptor().after(ewi())
                        .interceptor(new ReplaceLatchInterceptor(replaceStartedLatch, replaceProceedLatch));

      // do not allow coordinator to send topology updates to node B
      final ClusterTopologyManager ctm0 = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      ctm0.setRebalancingEnabled(false);

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      log.info("Added a new node");

      // node B is not a member yet and rebalance has not started yet
      CacheTopology cacheTopology = advancedCache(1).getDistributionManager().getCacheTopology();
      assertNull(cacheTopology.getPendingCH());
      assertTrue(cacheTopology.getMembers().contains(address(0)));
      assertFalse(cacheTopology.getMembers().contains(address(1)));
      assertFalse(cacheTopology.getCurrentCH().getMembers().contains(address(1)));

      // no keys should be present on node B yet because state transfer is blocked
      assertTrue(cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().isEmpty());

      // initiate a REPLACE
      Future<Object> getFuture = fork(() -> {
         try {
            return cache(1).replace("myKey", "newValue");
         } catch (Exception e) {
            log.errorf(e, "REPLACE failed: %s", e.getMessage());
            throw e;
         }
      });

      // wait for REPLACE command on node B to reach beyond *EntryWrappingInterceptor, where it will block.
      // the value seen so far is null
      if (!replaceStartedLatch.await(10, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // paranoia, yes the value is still missing from data container
      assertTrue(cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().isEmpty());

      // allow rebalance to start
      ctm0.setRebalancingEnabled(true);

      // wait for state transfer to end
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      // the state should be already transferred now
      assertEquals(1, cache(1).keySet().size());

      // allow REPLACE to continue
      replaceProceedLatch.countDown();

      Object oldVal = getFuture.get(10, TimeUnit.SECONDS);
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
      cacheConfigBuilder.customInterceptors().addInterceptor().before(InvocationContextInterceptor.class)
                        .interceptor(new StateTransferLatchInterceptor(applyStateStartedLatch, applyStateProceedLatch));

      // add an interceptor on node B that will block GET commands until we are ready
      final CountDownLatch getKeyStartedLatch = new CountDownLatch(1);
      final CountDownLatch getKeyProceedLatch = new CountDownLatch(1);
      cacheConfigBuilder.customInterceptors().addInterceptor().before(CallInterceptor.class)
                        .interceptor(new GetLatchInterceptor(getKeyStartedLatch, getKeyProceedLatch));

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      log.info("Added a new node");

      // Note: We have to access DC instead of cache with LOCAL_MODE flag
      // state transfer is blocked, no keys should be present on node B yet
      assertEquals(0, cache(1).getAdvancedCache().getDataContainer().size());

      // wait for state transfer on node B to progress to the point where data segments are about to be applied
      if (!applyStateStartedLatch.await(10, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // state transfer is blocked, no keys should be present on node B yet
      assertEquals(0, cache(1).getAdvancedCache().getDataContainer().size());

      // initiate a GET
      Future<Object> getFuture = fork(() -> cache(1).get("myKey"));

      // wait for GET command on node B to reach beyond *DistributionInterceptor, where it will block.
      // the value seen so far is null
      if (!getKeyStartedLatch.await(10, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      // allow state transfer to apply state
      applyStateProceedLatch.countDown();

      // wait for state transfer to end
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      assertEquals(1, cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().size());

      // allow GET to continue
      getKeyProceedLatch.countDown();

      Object value = getFuture.get(10, TimeUnit.SECONDS);
      assertEquals("myValue", value);
   }

   static class RemoveLatchInterceptor extends BaseAsyncInterceptor {
      private final CountDownLatch removeStartedLatch;
      private final CountDownLatch removeProceedLatch;

      public RemoveLatchInterceptor(CountDownLatch removeStartedLatch, CountDownLatch removeProceedLatch) {
         this.removeStartedLatch = removeStartedLatch;
         this.removeProceedLatch = removeProceedLatch;
      }

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
         if (cmd instanceof RemoveCommand) {
            // signal we encounter a REMOVE
            removeStartedLatch.countDown();
            // wait until it is ok to continue with REMOVE
            if (!removeProceedLatch.await(10, TimeUnit.SECONDS)) {
               throw new TimeoutException();
            }
         }
         return invokeNext(ctx, cmd);
      }
   }

   static class PutLatchInterceptor extends BaseAsyncInterceptor {
      private final CountDownLatch putStartedLatch;
      private final CountDownLatch putProceedLatch;

      public PutLatchInterceptor(CountDownLatch putStartedLatch, CountDownLatch putProceedLatch) {
         this.putStartedLatch = putStartedLatch;
         this.putProceedLatch = putProceedLatch;
      }

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
         if (cmd instanceof PutKeyValueCommand &&
             !((PutKeyValueCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            // signal we encounter a (non-state-transfer) PUT
            putStartedLatch.countDown();
            // wait until it is ok to continue with PUT
            if (!putProceedLatch.await(10, TimeUnit.SECONDS)) {
               throw new TimeoutException();
            }
         }
         return invokeNext(ctx, cmd);
      }
   }

   static class ReplaceLatchInterceptor extends BaseAsyncInterceptor {
      private final CountDownLatch replaceStartedLatch;
      private final CountDownLatch replaceProceedLatch;

      public ReplaceLatchInterceptor(CountDownLatch replaceStartedLatch, CountDownLatch replaceProceedLatch) {
         this.replaceStartedLatch = replaceStartedLatch;
         this.replaceProceedLatch = replaceProceedLatch;
      }

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
         if (cmd instanceof ReplaceCommand) {
            // signal we encounter a REPLACE
            replaceStartedLatch.countDown();
            // wait until it is ok to continue with REPLACE
            if (!replaceProceedLatch.await(10, TimeUnit.SECONDS)) {
               throw new TimeoutException();
            }
         }
         return invokeNext(ctx, cmd);
      }
   }

   static class StateTransferLatchInterceptor extends BaseAsyncInterceptor {
      private final CountDownLatch applyStateStartedLatch;
      private final CountDownLatch applyStateProceedLatch;

      public StateTransferLatchInterceptor(CountDownLatch applyStateStartedLatch,
                                           CountDownLatch applyStateProceedLatch) {
         this.applyStateStartedLatch = applyStateStartedLatch;
         this.applyStateProceedLatch = applyStateProceedLatch;
      }

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
         // if this 'put' command is caused by state transfer we block until GET begins
         if (cmd instanceof PutKeyValueCommand &&
             ((PutKeyValueCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            // signal we encounter a state transfer PUT
            applyStateStartedLatch.countDown();
            // wait until it is ok to apply state
            if (!applyStateProceedLatch.await(10, TimeUnit.SECONDS)) {
               throw new TimeoutException();
            }
         }
         return invokeNext(ctx, cmd);
      }
   }

   static class GetLatchInterceptor extends BaseAsyncInterceptor {
      private final CountDownLatch getKeyStartedLatch;
      private final CountDownLatch getKeyProceedLatch;

      public GetLatchInterceptor(CountDownLatch getKeyStartedLatch, CountDownLatch getKeyProceedLatch) {
         this.getKeyStartedLatch = getKeyStartedLatch;
         this.getKeyProceedLatch = getKeyProceedLatch;
      }

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
         if (cmd instanceof GetKeyValueCommand) {
            // Only block the first get to come here - they are not concurrent so this check is fine
            if (getKeyStartedLatch.getCount() != 0) {
               // signal we encounter a GET
               getKeyStartedLatch.countDown();
               // wait until it is ok to continue with GET
               if (!getKeyProceedLatch.await(10, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
         }
         return invokeNext(ctx, cmd);
      }
   }
}
