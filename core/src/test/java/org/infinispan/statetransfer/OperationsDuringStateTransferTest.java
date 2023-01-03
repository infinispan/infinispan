package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.ControllerBlockingInterceptor;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.BiasedEntryWrappingInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.interceptors.impl.RetryingEntryWrappingInterceptor;
import org.infinispan.interceptors.impl.VersionedEntryWrappingInterceptor;
import org.infinispan.remoting.inboundhandler.BlockHandler;
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
         new OperationsDuringStateTransferTest().cacheMode(CacheMode.SCATTERED_SYNC).transactional(false).biasAcquisition(BiasAcquisition.NEVER),
         new OperationsDuringStateTransferTest().cacheMode(CacheMode.SCATTERED_SYNC).transactional(false).biasAcquisition(BiasAcquisition.ON_WRITE),
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
      if (biasAcquisition != null) {
         cacheConfigBuilder.clustering().biasAcquisition(biasAcquisition);
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
      BlockHandler removeController = blockAfterInterceptor(RemoveCommand.class::isInstance, ewi());

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
      removeController.awaitUntilBlocked();

      // paranoia, yes the value is still missing from data container
      assertTrue(cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().isEmpty());

      // allow rebalance to start
      ctm0.setRebalancingEnabled(true);

      // wait for state transfer to end
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      // the state should be already transferred now
      assertEquals(1, cache(1).keySet().size());

      // allow REMOVE to continue
      removeController.unblock();

      Object oldVal = getFuture.get(10, TimeUnit.SECONDS);
      assertNotNull(oldVal);
      assertEquals("myValue", oldVal);

      assertNull(cache(0).get("myKey"));
      assertNull(cache(1).get("myKey"));
   }

   public Class<? extends DDAsyncInterceptor> ewi() {
      Class<? extends DDAsyncInterceptor> after;
      if (cacheMode.isScattered()) {
         after = biasAcquisition == BiasAcquisition.NEVER ? RetryingEntryWrappingInterceptor.class : BiasedEntryWrappingInterceptor.class;
      } else if (Configurations.isTxVersioned(cache(0).getCacheConfiguration())) {
         after = VersionedEntryWrappingInterceptor.class;
      } else {
         after = EntryWrappingInterceptor.class;
      }
      return after;
   }

   public void testPut() throws Exception {
      cache(0).put("myKey", "myValue");

      // add an interceptor on second node that will block PUT commands right after EntryWrappingInterceptor until we are ready
      BlockHandler putController = blockAfterInterceptor(OperationsDuringStateTransferTest::isNormalPut, ewi());

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
      putController.awaitUntilBlocked();

      // paranoia, yes the value is still missing from data container
      assertTrue(cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().isEmpty());

      // allow rebalance to start
      ctm0.setRebalancingEnabled(true);

      // wait for state transfer to end
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      // the state should be already transferred now
      assertEquals(1, cache(1).keySet().size());

      // allow PUT to continue
      putController.unblock();

      Object oldVal = putFuture.get(10, TimeUnit.SECONDS);
      assertNotNull(oldVal);
      assertEquals("myValue", oldVal);

      assertEquals("newValue", cache(0).get("myKey"));
      assertEquals("newValue", cache(1).get("myKey"));
   }

   public void testReplace() throws Exception {
      cache(0).put("myKey", "myValue");

      // add an interceptor on second node that will block REPLACE commands right after EntryWrappingInterceptor until we are ready
      BlockHandler replaceController = blockAfterInterceptor(ReplaceCommand.class::isInstance, ewi());

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
      replaceController.awaitUntilBlocked();

      // paranoia, yes the value is still missing from data container
      assertTrue(cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().isEmpty());

      // allow rebalance to start
      ctm0.setRebalancingEnabled(true);

      // wait for state transfer to end
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      // the state should be already transferred now
      assertEquals(1, cache(1).keySet().size());

      // allow REPLACE to continue
      replaceController.unblock();

      Object oldVal = getFuture.get(10, TimeUnit.SECONDS);
      assertNotNull(oldVal);
      assertEquals("myValue", oldVal);

      assertEquals("newValue", cache(0).get("myKey"));
      assertEquals("newValue", cache(1).get("myKey"));
   }

   public void testGet() throws Exception {
      cache(0).put("myKey", "myValue");

      // add an interceptor on node B that will block state transfer until we are ready
      BlockHandler stateTransferController = ControllerBlockingInterceptor.addBefore(cacheConfigBuilder, InvocationContextInterceptor.class)
            .blockCommand(OperationsDuringStateTransferTest::isStateTransferPut);

      // add an interceptor on node B that will block GET commands until we are ready
      BlockHandler getController = blockGet();

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      log.info("Added a new node");

      // Note: We have to access DC instead of cache with LOCAL_MODE flag, as scattered mode cache would
      // already become an owner and would wait for the state transfer
      // state transfer is blocked, no keys should be present on node B yet
      assertEquals(0, cache(1).getAdvancedCache().getDataContainer().size());

      // wait for state transfer on node B to progress to the point where data segments are about to be applied
      stateTransferController.awaitUntilBlocked();

      // state transfer is blocked, no keys should be present on node B yet
      assertEquals(0, cache(1).getAdvancedCache().getDataContainer().size());

      // initiate a GET
      Future<Object> getFuture = fork(() -> cache(1).get("myKey"));

      // wait for GET command on node B to reach beyond *DistributionInterceptor, where it will block.
      // the value seen so far is null
      getController.awaitUntilBlocked();

      // allow state transfer to apply state
      stateTransferController.unblock();

      // wait for state transfer to end
      TestingUtil.waitForNoRebalance(cache(0), cache(1));

      assertEquals(1, cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet().size());

      // allow GET to continue
      getController.unblock();

      Object value = getFuture.get(10, TimeUnit.SECONDS);
      assertEquals("myValue", value);
   }

   private BlockHandler blockAfterInterceptor(Predicate<VisitableCommand> test, Class<? extends AsyncInterceptor> afterInterceptor) {
      ControllerBlockingInterceptor interceptor = ControllerBlockingInterceptor.addAfter(cacheConfigBuilder, afterInterceptor);
      return interceptor.blockCommand(test);
   }

   private BlockHandler blockGet() {
      // we cannot have 2 interceptors of the same class.
      GetBlockInterceptor getBlockInterceptor = new GetBlockInterceptor();
      cacheConfigBuilder.customInterceptors().addInterceptor().before(CallInterceptor.class)
            .interceptor(getBlockInterceptor);
      return getBlockInterceptor.blockCommand(GetKeyValueCommand.class);
   }

   private static boolean isNormalPut(VisitableCommand cmd) {
      return cmd instanceof PutKeyValueCommand && !((PutKeyValueCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER);
   }

   private static boolean isStateTransferPut(VisitableCommand cmd) {
      return cmd instanceof PutKeyValueCommand && ((PutKeyValueCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER);
   }

   // need a second interceptor because we don't support having the same interceptor class twice or more
   static class GetBlockInterceptor extends ControllerBlockingInterceptor {

   }
}
