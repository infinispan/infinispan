package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commands.topology.RebalanceStatusRequestCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(testName = "topology.ClusterTopologyViewChangesTest", groups = "functional")
public class ClusterTopologyViewChangesTest extends MultipleCacheManagersTest {

   private final int dataSize = 100;


   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(defaultGlobalConfig(), defaultCacheConfig(), 2);
   }

   private GlobalConfigurationBuilder defaultGlobalConfig() {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.transport().distributedSyncTimeout(30, TimeUnit.SECONDS);
      return gcb;
   }

   private ConfigurationBuilder defaultCacheConfig() {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cb.clustering().stateTransfer().timeout(15, TimeUnit.SECONDS);
      return cb;
   }

   private EmbeddedCacheManager addNewMember() {
      return addClusterEnabledCacheManager(defaultGlobalConfig(), defaultCacheConfig());
   }

   public void nodeLeftDuringCacheJoin() throws Exception {
      executeJoinTest(findNonCoordinatorIndex());
   }

   public void coordinatorLeftDuringCacheJoin() throws Exception {
      executeJoinTest(findCoordinatorIndex());
   }

   public void concurrentJoin() throws Exception {
      executeJoinTest(-1);
   }

   public void nodeLeftDuringCacheJoinWithRebalanceDisabled() throws Exception {
      waitForClusterToForm();
      TestingUtil.extractGlobalComponentRegistry(findCoordinator())
            .getClusterTopologyManager()
            .setRebalancingEnabled(false)
            .toCompletableFuture().get(10, TimeUnit.SECONDS);

      executeJoinTest(findNonCoordinatorIndex());
   }

   public void coordinatorLeftDuringCacheJoinWithRebalanceDisabled() throws Exception {
      waitForClusterToForm();
      TestingUtil.extractGlobalComponentRegistry(findCoordinator())
            .getClusterTopologyManager()
            .setRebalancingEnabled(false)
            .toCompletableFuture().get(10, TimeUnit.SECONDS);

      executeJoinTest(findCoordinatorIndex());
   }

   private void executeJoinTest(int nodeToStop) throws Exception {
      // First step, populate the cache.
      populateCache();

      CheckPoint checkPoint = new CheckPoint();
      AtomicBoolean onlyOnce = new AtomicBoolean(true);

      // Replace to block on the first status request command to retrieve the global status.
      Mocks.blockInboundGlobalCommand(findCoordinator(), checkPoint, rpc -> {
         if (rpc instanceof RebalanceStatusRequestCommand) {
            RebalanceStatusRequestCommand rsrc = (RebalanceStatusRequestCommand) rpc;
            return rsrc.getCacheName() == null && onlyOnce.getAndSet(false);
         }
         return false;
      });

      waitForClusterToForm();

      Future<EmbeddedCacheManager> joining = fork(this::addNewMember);

      // Rebalance received but not yet executed.
      checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      if (nodeToStop < 0) {
         addNewMember();
      } else {
         TestingUtil.killCacheManagers(cacheManagers.remove(nodeToStop));
      }

      // Release the request command.
      checkPoint.trigger(Mocks.BEFORE_RELEASE);
      checkPoint.trigger(Mocks.AFTER_RELEASE);

      log.info("Waiting for joiner to finish");
      EmbeddedCacheManager joiner = joining.get(10, TimeUnit.SECONDS);

      // Assert the joiner has the same status as the coordinator.
      boolean rebalanced = TestingUtil.extractGlobalComponentRegistry(joiner).getClusterTopologyManager().isRebalancingEnabled();
      assertThat(TestingUtil.extractGlobalComponentRegistry(findCoordinator()).getClusterTopologyManager().isRebalancingEnabled())
            .isEqualTo(rebalanced);

      // Assert data on all caches if rebalance enabled.
      if (rebalanced) assertCacheData();
   }

   private void populateCache() {
      Cache<String, String> cache = cache(0);
      IntStream.range(0, dataSize).parallel()
            .forEach(i -> cache.put("key-" + i, "value-" + i));
   }

   private void assertCacheData() {
      for (int m = 0; m < managers().length; m++) {
         Cache<String, String> cache = cache(m);
         int size = cache.size();
         assertThat(size)
               .withFailMessage(String.format("Cache %d has %d/%d entries", m, size, dataSize))
               .isEqualTo(dataSize);

         for (int i = 0; i < dataSize; i++) {
            assertThat(cache.get("key-" + i)).isEqualTo("value-" + i);
         }
      }
   }

   private EmbeddedCacheManager findCoordinator() {
      return manager(findCoordinatorIndex());
   }

   private int findCoordinatorIndex() {
      for (int i = 0; i < managers().length; i++) {
         if (manager(i).isCoordinator()) return i;
      }

      throw new IllegalStateException("Coordinator node not found");
   }

   private int findNonCoordinatorIndex() {
      for (int i = 0; i < managers().length; i++) {
         if (!manager(i).isCoordinator()) return i;
      }

      throw new IllegalStateException("There are only coordinators?");
   }
}
