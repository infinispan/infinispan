package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.nio.file.Paths;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(testName = "topology.StatefulShutdownDuringJoinTest", groups = "functional")
public class StatefulShutdownDuringJoinTest extends AbstractStatefulCluster {

   {
      clusterSize = 2;
   }

   private final int dataSize = 100;

   @Override
   protected ConfigurationBuilder createCacheConfig(String id) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);

      // We want persistence so data survive through restarts.
      builder.persistence().addSoftIndexFileStore()
            .dataLocation(Paths.get(stateDirectory, "data").toString())
            .indexLocation(Paths.get(stateDirectory, "index").toString());
      return builder;
   }

   private void shutdownBeforeJoiningComplete() throws Exception {
      // Causes node A to join.
      // This would create the cache status and persist the state on shutdown.
      Cache<Object, Object> c0 = manager(0).getCache(cacheName);

      assertThat(manager(0).isCoordinator()).isTrue();

      // Cache is operating normally.
      fillData(c0);

      // Checkpoint to intercept the mock invocations during the join.
      CheckPoint checkPoint = new CheckPoint();
      ClusterTopologyManager originalCtm = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      ClusterTopologyManager spyCtm = spy(originalCtm);
      doAnswer(ivk -> {
         Object o = ivk.callRealMethod();
         checkPoint.trigger(Mocks.AFTER_INVOCATION);
         checkPoint.awaitStrict(Mocks.AFTER_RELEASE, 5, TimeUnit.SECONDS);
         return o;
      }).when(spyCtm).handleJoin(eq(cacheName), any(), any(), anyInt());

      // We only replace one way. We'll shut down this manager later.
      TestingUtil.replaceComponent(manager(0), ClusterTopologyManager.class, spyCtm, true);

      // Asynchronous call to wait. This would block until the cache join request finishes.
      Future<Void> create = fork(() -> waitForClusterToForm(cacheName));

      // Wait until the node join is PROCESSED but not yet REPLIED.
      checkPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS);

      // Node B still haven't joined.
      assertThat(create.isDone()).isFalse();

      // Shutdown everything. This should create the state files.
      // Since node B haven't joined, the operation will fail on B but succeed on A.
      c0.shutdown();

      // Kill all the managers to recreate cluster.
      // Observe that we NEVER released the checkpoint, it times out.
      // That is, the cache was shutdown before the join response is received, the topology was never updated and only
      // one of the nodes succeeded writing the state to file.
      TestingUtil.killCacheManagers(managers());
      eventually(create::isDone, 10, TimeUnit.SECONDS);
   }

   public void testRestartStatelessCoordinator() throws Exception {
      // Shutdown before node B joins node A.
      // This causes node A to have a state file and node B does not.
      shutdownBeforeJoiningComplete();

      // All nodes should have a state file.
      assertClusterStateFiles();

      // But only node A has the state file for the cache.
      assertClusterStateFiles(manager(0), cacheName);
      cacheManagers.clear();

      // Now we create the cluster backwards. Node B will be the coordinator.
      // This will cause the node B, which doesn't have the cache state to be the coordinator.
      createStatefulCacheManager(false, "B");
      assertThat(manager(0).isCoordinator()).isTrue();

      // Retrieve cache to create only on node B first.
      Cache<Object, Object> c1 = manager(0).getCache(cacheName);
      c1.put("k-0", "v1");

      // We now create node A again. Since it has a persistent state, the join will fail.
      createStatefulCacheManager(false, "A");
      Assertions.assertThatThrownBy(() -> waitForClusterToForm(cacheName))
            .rootCause()
            .isInstanceOf(CacheJoinException.class)
            .hasMessageStartingWith("ISPN000408:");

      // Check it is still operational. This is on node B.
      assertThat(c1.get("k-0")).isEqualTo("v1");
      assertThat(c1.size()).isOne();
   }

   public void testRestartStatefulCoordinatorAndStatelessBackup() throws Exception {
      // Shutdown before node B joins node A.
      // This causes node A to have a state file and node B does not.
      shutdownBeforeJoiningComplete();

      // All nodes should have a state file.
      assertClusterStateFiles();

      // But only node A has the state file for the cache.
      assertClusterStateFiles(manager(0), cacheName);
      cacheManagers.clear();

      // First, start the coordinator. This node has a state file for the cache.
      createStatefulCacheManager(false, "A");
      assertThat(manager(0).isCoordinator()).isTrue();

      // We now start the backup node, which does not have a state file.
      createStatefulCacheManager(false, "B");
      waitForClusterToForm(cacheName);

      // The data still present.
      // We check with the backup node cache.
      assertData(manager(1).getCache(cacheName));
   }

   private void fillData(Cache<Object, Object> cache) {
      for (int i = 0; i < dataSize; i++) {
         cache.put("k-" + i, "v-" + i);
      }
   }

   private void assertData(Cache<Object, Object> cache) {
      assertThat(cache.size()).isEqualTo(dataSize);
      for (int i = 0; i < dataSize; i++) {
         assertThat(cache.get("k-" + i)).isEqualTo("v-" + i);
      }
   }
}
