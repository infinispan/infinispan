package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "topology.ClusterTopologyStatefulTest", groups = "functional")
public class ClusterTopologyStatefulTest extends AbstractStatefulCluster {

   {
      clusterSize = 3;
   }

   private Condition allNodesHaveRebalanceDisabled() {
      return () -> Arrays.stream(managers())
            .map(ecm -> TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class))
            .noneMatch(ClusterTopologyManager::isRebalancingEnabled);
   }

   private Supplier<String> dumpClusterGlobalRebalanceStatus() {
      return () -> Arrays.stream(managers())
            .map(ecm -> Map.entry(ecm, TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class)))
            .map(entry -> String.format("%s is rebalance enabled? %b", entry.getKey().getAddress(), entry.getValue().isRebalancingEnabled()))
            .collect(Collectors.joining(System.lineSeparator()));
   }

   private void disableRebalanceAndShutdown() throws Exception {
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);

      // Disable rebalance globally.
      ctm.setRebalancingEnabled(false).toCompletableFuture().get(10, TimeUnit.SECONDS);

      // Assert that eventually the whole cluster has rebalance disabled.
      eventually(dumpClusterGlobalRebalanceStatus(), allNodesHaveRebalanceDisabled());

      // Shutdown everything. This will generate the state file.
      TestingUtil.killCacheManagers(this.cacheManagers);

      // Verify all managers have a global state file.
      assertClusterStateFiles();
      this.cacheManagers.clear();
   }

   public void testRebalanceAfterRestart() throws Exception {
      disableRebalanceAndShutdown();

      // Recreate the cluster.
      createStatefulCacheManager(false);

      // All nodes should start with global rebalance disabled.
      assertThat(allNodesHaveRebalanceDisabled().isSatisfied())
            .as(dumpClusterGlobalRebalanceStatus())
            .isTrue();
   }

   public void testOnlyCoordinatorKeepsGlobalState() throws Exception {
      disableRebalanceAndShutdown();

      // Now we create only the coordinator. The coordinator restore with the global state.
      createStatefulCacheManager(false, "A");

      // Assert the coordinator has restored the state.
      assertThat(allNodesHaveRebalanceDisabled().isSatisfied())
            .as(dumpClusterGlobalRebalanceStatus())
            .isTrue();

      // Now we restore the remaining nodes. Note that they do not keep the global state.
      // These nodes must recover the rebalance status from the coordinator A.
      for (int i = 1; i < clusterSize; i++) {
         createStatefulCacheManager(true, Character.toString('A' + i));
      }

      // All nodes should start with global rebalance disabled.
      assertThat(allNodesHaveRebalanceDisabled().isSatisfied())
            .as(dumpClusterGlobalRebalanceStatus())
            .isTrue();
   }
}
