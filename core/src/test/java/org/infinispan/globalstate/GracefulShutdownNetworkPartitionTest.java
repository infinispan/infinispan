package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.BaseStatefulPartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.jgroups.JChannel;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.LOCAL_PING;

@CleanupAfterMethod
public class GracefulShutdownNetworkPartitionTest extends BaseStatefulPartitionHandlingTest {

   {
      partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
      numMembersInCluster = 5;
      DATA_SIZE = 1000;
      createDefault = false;
   }

   public void testNetworkPartitionWhenRestartingFromShutdown() throws Throwable {
      createInitialCluster();
      cache(0, CACHE_NAME).shutdown();

      TestingUtil.killCacheManagers(this.cacheManagers);
      assertStateFiles();
      cacheManagers.clear();

      createStatefulCacheManagers(false);

      // All but one member join the cluster. The cache is still not recovered.
      for (int i = 0; i < numMembersInCluster - 1; i++) {
         cache(i, CACHE_NAME);
      }

      final LocalTopologyManager ltm = extractGlobalComponent(manager(0), LocalTopologyManager.class);
      assertThat(ltm.getCacheTopology(CACHE_NAME)).isNull();
      assertThat(ltm.getStableCacheTopology(CACHE_NAME)).isNull();

      // We cause a network partition.
      // The node that needs to join is in the same partition as the coordinator.
      splitCluster(new int[]{ 0, 3, 4 }, new int[]{ 1, 2 });
      eventually(() -> {
         List<Address> members = manager(0).getMembers();
         return !members.containsAll(List.of(manager(1).getAddress(), manager(2).getAddress()));
      });

      // During the network partition, the last node joins.
      cache(numMembersInCluster - 1, CACHE_NAME);

      assertThat(ltm.isCacheRebalancingEnabled(CACHE_NAME)).isTrue();

      partition(0).merge(partition(1), false);
      eventually(() -> cacheManagers.stream().allMatch(ecm -> ecm.getMembers().size() == numMembersInCluster));
      eventually(() -> cacheManagers.stream()
            .map(ecm -> extractGlobalComponent(ecm, LocalTopologyManager.class))
            .allMatch(localTopologyManager -> {
               CacheTopology ct = localTopologyManager.getCacheTopology(CACHE_NAME);
               return ct != null && ct.getMembers().size() == numMembersInCluster;
            }), 30, TimeUnit.SECONDS);

      int survivingData = cache(0, CACHE_NAME).size();
      assertThat(survivingData)
            .as("All data should survive graceful shutdown restart, even with partition")
            .isEqualTo(DATA_SIZE);
      assertCacheUsable();
   }

   public void testCoordinatorLeavesDuringPartition() throws Throwable {
      actualTest(0);
   }

   public void testNodeLeavesDuringPartition() throws Throwable {
      actualTest(1);
   }

   public void testCoordinatorLeavesAfterRecovery() throws Throwable {
      createInitialCluster();
      cache(0, CACHE_NAME).shutdown();

      TestingUtil.killCacheManagers(this.cacheManagers);
      assertStateFiles();
      cacheManagers.clear();

      createStatefulCacheManagers(false);

      log.info("Waiting for cluster to form again");
      // All members join and recovery completes (Phase 1 + Phase 2).
      waitForClusterToForm(CACHE_NAME);
      checkData();

      // Verify state files are deleted (Phase 2 completed).
      for (int i = 0; i < numMembersInCluster; i++) {
         String loc = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] stateFiles = new File(loc).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertThat(stateFiles).as("State file should be deleted after Phase 2").isNullOrEmpty();
      }

      // Kill the coordinator.
      EmbeddedCacheManager killed = cacheManagers.remove(0);
      TestingUtil.killCacheManagers(killed);

      // The remaining cluster should converge without delays.
      eventually(() -> cacheManagers.stream().allMatch(ecm -> ecm.getMembers().size() == numMembersInCluster - 1));
      eventually(() -> cacheManagers.stream()
            .map(ecm -> extractGlobalComponent(ecm, LocalTopologyManager.class))
            .allMatch(l -> {
               CacheTopology ct = l.getCacheTopology(CACHE_NAME);
               return ct != null && ct.getMembers().size() == numMembersInCluster - 1;
            }), 30, TimeUnit.SECONDS);

      // No data loss.
      int survivingData = cache(0, CACHE_NAME).size();
      assertThat(survivingData)
            .as("All data should survive coordinator leave after successful recovery")
            .isEqualTo(DATA_SIZE);

      assertCacheUsable();
   }

   private void actualTest(int nodeToKill) throws Throwable {
      createInitialCluster();
      cache(0, CACHE_NAME).shutdown();

      TestingUtil.killCacheManagers(this.cacheManagers);
      assertStateFiles();
      cacheManagers.clear();

      createStatefulCacheManagers(false);

      for (int i = 0; i < numMembersInCluster - 1; i++) {
         cache(i, CACHE_NAME);
      }

      LocalTopologyManager ltm = extractGlobalComponent(manager(0), LocalTopologyManager.class);
      assertThat(ltm.getCacheTopology(CACHE_NAME)).isNull();

      // Partition: {0, 3, 4} vs {1, 2}
      splitCluster(new int[]{0, 3, 4}, new int[]{1, 2});
      eventually(() -> {
         List<Address> members = manager(0).getMembers();
         return !members.containsAll(List.of(manager(1).getAddress(), manager(2).getAddress()));
      });

      cache(numMembersInCluster - 1, CACHE_NAME);

      // Kill the specified node while partition is still active
      EmbeddedCacheManager killed = cacheManagers.remove(nodeToKill);
      Address killedAddr = killed.getAddress();
      JChannel killedChannel = channel(killed);
      TestingUtil.killCacheManagers(killed);
      for (int pi = 0; pi < 2; pi++) {
         if (partition(pi).channels().remove(killedChannel)) break;
      }

      // Wait for surviving nodes to detect the leave, then merge
      eventually(() -> cacheManagers.stream().noneMatch(ecm -> ecm.getMembers().contains(killedAddr)));
      partition(0).merge(partition(1), false);
      eventually(() -> cacheManagers.stream().allMatch(ecm -> ecm.getMembers().size() == numMembersInCluster - 1));

      // Remove DISCARD from all surviving channels
      for (int i = 0; i < cacheManagers.size(); i++) {
         channel(manager(i)).getProtocolStack().removeProtocol(DISCARD.class);
      }

      // Fix LOCAL_PING stale entries: clear and re-register coordinator first.
      LOCAL_PING.clear();
      for (int i = 0; i < cacheManagers.size(); i++) {
         JChannel ch = channel(manager(i));
         if (ch.getAddress().equals(ch.getView().getCoord())) {
            ((LOCAL_PING) ch.getProtocolStack().findProtocol(LOCAL_PING.class)).handleConnect();
            break;
         }
      }
      for (int i = 0; i < cacheManagers.size(); i++) {
         JChannel ch = channel(manager(i));
         if (!ch.getAddress().equals(ch.getView().getCoord())) {
            ((LOCAL_PING) ch.getProtocolStack().findProtocol(LOCAL_PING.class)).handleConnect();
         }
      }

      // Restart killed node with its original state directory
      String stateId = Character.toString((char) ('A' + nodeToKill));
      createStatefulCacheManager(stateId, false);
      int restartedIndex = cacheManagers.size() - 1;
      CompletableFuture<Void> join = CompletableFuture.runAsync(() -> cache(restartedIndex, CACHE_NAME), testExecutor());

      // All nodes converge
      eventually(() -> cacheManagers.stream().allMatch(ecm -> ecm.getMembers().size() == numMembersInCluster));
      eventually(() -> cacheManagers.stream()
            .map(ecm -> extractGlobalComponent(ecm, LocalTopologyManager.class))
            .allMatch(l -> {
               CacheTopology ct = l.getCacheTopology(CACHE_NAME);
               return ct != null && ct.getMembers().size() == numMembersInCluster;
            }), 30, TimeUnit.SECONDS);

      TestingUtil.join(join);

      // No data loss
      int survivingData = cache(0, CACHE_NAME).size();
      assertThat(survivingData)
            .as("All data should survive graceful shutdown restart, even with node leave during partition")
            .isEqualTo(DATA_SIZE);
      assertCacheUsable();
   }

   private void assertStateFiles() {
      for (int i = 0; i < numMembersInCluster; i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertEquals(Arrays.toString(listFiles), 1, listFiles.length);
      }
   }

   private void assertCacheUsable() {
      // Verify cache is still usable after recovery.
      cache(0, CACHE_NAME).put("post-recovery-key", "post-recovery-value");
      assertThat(cache(1, CACHE_NAME).get("post-recovery-key")).isEqualTo("post-recovery-value");
      cache(2, CACHE_NAME).put("another-key", "another-value");
      assertThat(cache(0, CACHE_NAME).get("another-key")).isEqualTo("another-value");
   }
}
