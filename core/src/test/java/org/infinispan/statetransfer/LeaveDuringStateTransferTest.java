package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.BlockingClusterTopologyManager;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "statetransfer.LeaveDuringStateTransferTest", description = "One instance of ISPN-5021")
public class LeaveDuringStateTransferTest extends MultipleCacheManagersTest {

   private ControlledConsistentHashFactory factory = new ControlledConsistentHashFactory(0, 1);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = configuration();
      createClusteredCaches(3, cb, new TransportFlags().withFD(true));
   }

   private ConfigurationBuilder configuration() {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      cb.clustering().hash().numSegments(1).consistentHashFactory(factory);
      return cb;
   }

   public void test() throws Exception {
      BlockingClusterTopologyManager clusterTopologyManager = BlockingClusterTopologyManager.replace(cacheManagers.get(0));
      BlockingLocalTopologyManager localTopologyManager0 = BlockingLocalTopologyManager.replaceTopologyManager(cacheManagers.get(0));
      BlockingLocalTopologyManager localTopologyManager2 = BlockingLocalTopologyManager.replaceTopologyManager(cacheManagers.get(2));

      int currentTopology = currentTopologyId(cache(0));
      // block last CH_UPDATE in the state transfer
      BlockingClusterTopologyManager.Handle h3 = clusterTopologyManager.startBlockingTopologyConfirmations(
            topologyId -> topologyId >= currentTopology + 3);
      Future<?> joiner = null;

      try {
         factory.setOwnerIndexes(1, 2);
         addClusterEnabledCacheManager(configuration(), new TransportFlags().withFD(true));
         joiner = fork(() -> cacheManagers.get(3).getCache());

         h3.waitToBlock();

         log.debug("State transfer almost complete");

         eventually(() -> currentTopologyId(cache(2)) == currentTopology + 3);
         localTopologyManager2.startBlocking(BlockingLocalTopologyManager.LatchType.CONSISTENT_HASH_UPDATE);
         localTopologyManager2.startBlocking(BlockingLocalTopologyManager.LatchType.REBALANCE);
         // Block rebalance that could follow even if the previous rebalance was not completed
         localTopologyManager0.startBlocking(BlockingLocalTopologyManager.LatchType.REBALANCE);

         log.debug("Isolating node " + cacheManagers.get(1));
         TestingUtil.getDiscardForCache(cache(1)).setDiscardAll(true);
         TestingUtil.blockUntilViewsReceived(60000, true, cacheManagers);

         log.debug("Waiting for topology update from view change");
         // since we're blocking confirmation for topology +3, the updated topology will be +4
         eventually(() -> currentTopologyId(cache(0)) >= currentTopology + 4);

         StateTransferLock originalLock = TestingUtil.extractComponent(cache(2), StateTransferLock.class);
         UnblockingStateTransferLock lock = new UnblockingStateTransferLock(originalLock, currentTopology + 4, localTopologyManager2);
         TestingUtil.replaceComponent(cache(2), StateTransferLock.class, lock, true);

         cache(0).put("key", "value");
         assertEquals("value", cache(2).get("key"));
      } finally {
         h3.stopBlocking();
         localTopologyManager2.stopBlocking(BlockingLocalTopologyManager.LatchType.CONSISTENT_HASH_UPDATE);
         localTopologyManager2.stopBlocking(BlockingLocalTopologyManager.LatchType.REBALANCE);
         localTopologyManager0.stopBlocking(BlockingLocalTopologyManager.LatchType.REBALANCE);
         if (joiner != null) joiner.get(10, TimeUnit.SECONDS);
      }
   }

   private int currentTopologyId(Cache cache) {
      return TestingUtil.extractComponent(cache, StateTransferManager.class).getCacheTopology().getTopologyId();
   }

   private class UnblockingStateTransferLock extends DelegatingStateTransferLock {
      private final int unblockingTopology;
      private final BlockingLocalTopologyManager localTopologyManager;

      public UnblockingStateTransferLock(StateTransferLock delegate, int unblockingTopology, BlockingLocalTopologyManager localTopologyManager) {
         super(delegate);
         this.unblockingTopology = unblockingTopology;
         this.localTopologyManager = localTopologyManager;
      }

      @Override
      public boolean transactionDataReceived(int expectedTopologyId) {
         log.info("Requesting topology " + expectedTopologyId);
         if (expectedTopologyId == unblockingTopology) {
            localTopologyManager.stopBlocking(BlockingLocalTopologyManager.LatchType.CONSISTENT_HASH_UPDATE);
         }
         return super.transactionDataReceived(expectedTopologyId);
      }
   }

}
