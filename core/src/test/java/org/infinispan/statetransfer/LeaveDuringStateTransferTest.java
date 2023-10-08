package org.infinispan.statetransfer;

import static org.infinispan.util.BlockingLocalTopologyManager.confirmTopologyUpdate;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "statetransfer.LeaveDuringStateTransferTest", description = "One instance of ISPN-5021")
public class LeaveDuringStateTransferTest extends MultipleCacheManagersTest {

   private final ControlledConsistentHashFactory.Default factory = new ControlledConsistentHashFactory.Default(0, 1);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = configuration();
      createClusteredCaches(3, ControlledConsistentHashFactory.SCI.INSTANCE, cb, new TransportFlags().withFD(true));
   }

   private ConfigurationBuilder configuration() {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      cb.clustering().hash().numSegments(1).consistentHashFactory(factory);
      return cb;
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   public void test() throws Exception {
      int startTopologyId = currentTopologyId(cache(0));

      BlockingLocalTopologyManager localTopologyManager0 =
         BlockingLocalTopologyManager.replaceTopologyManagerDefaultCache(cacheManagers.get(0));
      BlockingLocalTopologyManager localTopologyManager2 =
         BlockingLocalTopologyManager.replaceTopologyManagerDefaultCache(cacheManagers.get(2));

      try {
         factory.setOwnerIndexes(1, 2);
         addClusterEnabledCacheManager(ControlledConsistentHashFactory.SCI.INSTANCE, configuration(), new TransportFlags().withFD(true));
         Future<Cache<Object, Object>> joiner = fork(() -> cacheManagers.get(3).getCache());

         // Install READ_OLD, READ_ALL and READ_NEW topologies, but do not confirm READ_NEW (+3)
         confirmTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL, localTopologyManager0, localTopologyManager2);
         confirmTopologyUpdate(CacheTopology.Phase.READ_ALL_WRITE_ALL, localTopologyManager0, localTopologyManager2);
         localTopologyManager0.expectTopologyUpdate(CacheTopology.Phase.READ_NEW_WRITE_ALL).unblock();
         localTopologyManager2.expectTopologyUpdate(CacheTopology.Phase.READ_NEW_WRITE_ALL).unblock();
         BlockingLocalTopologyManager.BlockedConfirmation blockedConfirmation0 =
            localTopologyManager0.expectPhaseConfirmation();
         BlockingLocalTopologyManager.BlockedConfirmation blockedConfirmation2 =
            localTopologyManager2.expectPhaseConfirmation();

         log.debug("State transfer almost complete");
         eventually(() -> currentTopologyId(cache(2)) == startTopologyId + 3);
         // Block rebalance that could follow even if the previous rebalance was not completed

         log.debug("Isolating node " + cacheManagers.get(1));
         TestingUtil.getDiscardForCache(manager(1)).discardAll(true);
         TestingUtil.blockUntilViewsReceived(60000, true, cacheManagers);

         log.debug("Waiting for topology update from view change");
         // The coordinator sends a READ_NEW topology (+4), but doesn't wait for the confirmation
         // before restarting the rebalance with a READ_OLD topology update (+5).
         // Since the messages are not ordered, either one can be processed first.
         BlockingLocalTopologyManager.BlockedTopology blockedTopology0 = localTopologyManager0.expectTopologyUpdate();
         BlockingLocalTopologyManager.BlockedTopology blockedTopology2 = localTopologyManager2.expectTopologyUpdate();
         // The LimitedExecutor doesn't allow the new topology to be installed until the old confirmations are unblocked
         blockedConfirmation0.unblock();
         blockedConfirmation2.unblock();
         // Unblock the READ_NEW topology (+4) and keep the READ_OLD one (+5) blocked
         blockedTopology0 = blockNewRebalance(localTopologyManager0, blockedTopology0);
         blockedTopology2 = blockNewRebalance(localTopologyManager2, blockedTopology2);
         // since we blocked confirmation for topology +3, the new topology will be +4
         eventually(() -> currentTopologyId(cache(0)) == startTopologyId + 4);

         cache(0).put("key", "value");
         assertEquals("value", cache(2).get("key"));

         // Unblock and confirm READ_OLD topology (+5)
         blockedTopology0.unblock();
         blockedTopology2.unblock();
         localTopologyManager0.expectPhaseConfirmation().unblock();
         localTopologyManager2.expectPhaseConfirmation().unblock();
         // Finish the rebalance
         confirmTopologyUpdate(CacheTopology.Phase.READ_ALL_WRITE_ALL, localTopologyManager0, localTopologyManager2);
         confirmTopologyUpdate(CacheTopology.Phase.READ_NEW_WRITE_ALL, localTopologyManager0, localTopologyManager2);
         confirmTopologyUpdate(CacheTopology.Phase.NO_REBALANCE, localTopologyManager0, localTopologyManager2);

         joiner.get(10, TimeUnit.SECONDS);
      } finally {
         localTopologyManager2.stopBlocking();
         localTopologyManager0.stopBlocking();
      }
   }

   private BlockingLocalTopologyManager.BlockedTopology
   blockNewRebalance(BlockingLocalTopologyManager ltm, BlockingLocalTopologyManager.BlockedTopology blockedTopology)
      throws InterruptedException {
      if (blockedTopology.getCacheTopology().getPhase() == CacheTopology.Phase.READ_NEW_WRITE_ALL) {
         // Block the rebalance start first, and only then unblock the previous topology
         // Otherwise the order of the rebalance start and topology confirmation wouldn't be deterministic
         BlockingLocalTopologyManager.BlockedTopology newTopology =
            ltm.expectTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL);
         blockedTopology.unblock();
         ltm.expectPhaseConfirmation().unblock();
         return newTopology;
      } else {
         assertEquals(CacheTopology.Phase.READ_OLD_WRITE_ALL, blockedTopology.getCacheTopology().getPhase());
         ltm.confirmTopologyUpdate(CacheTopology.Phase.READ_NEW_WRITE_ALL);
         return blockedTopology;
      }
   }

   private int currentTopologyId(Cache cache) {
      return TestingUtil.extractComponent(cache, DistributionManager.class).getCacheTopology().getTopologyId();
   }
}
