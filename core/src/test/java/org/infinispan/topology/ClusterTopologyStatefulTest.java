package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "topology.ClusterTopologyStatefulTest", groups = "functional")
public class ClusterTopologyStatefulTest extends MultipleCacheManagersTest {

   private final int clusterSize = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCacheManager(true);
   }

   protected void createStatefulCacheManager(boolean clear) {
      for (int i = 0; i < clusterSize; i++) {
         createStatefulCacheManager(clear, Character.toString('A' + i));
      }
   }

   protected void createStatefulCacheManager(boolean clear, String id) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      if (clear) Util.recursiveFileRemove(stateDirectory);

      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      addClusterEnabledCacheManager(global, null);
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
      for (int i = 0; i < clusterSize; i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         try (Stream<Path> s = Files.list(Path.of(persistentLocation))) {
            assertThat(s.filter(p -> p.endsWith("___global.state"))).isNotEmpty();
         }
      }
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
