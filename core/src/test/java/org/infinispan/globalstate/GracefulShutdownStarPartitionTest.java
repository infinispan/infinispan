package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.partitionhandling.BaseStatefulPartitionHandlingTest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.View;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MutableDigest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "globalstate.GracefulShutdownStarPartitionTest")
public class GracefulShutdownStarPartitionTest extends BaseStatefulPartitionHandlingTest {

   {
      numMembersInCluster = 8;
      // More data makes it more likely to reproduce the issue.
      DATA_SIZE = 1000;
      createDefault = false;
   }

   public void testDataLossWithStarPartitionDuringGracefulRestart() throws Exception {
      // Phase 1: Create initial cluster and do graceful shutdown
      createInitialCluster();

      for (int i = 0; i < numMembersInCluster; i++) {
         ((DefaultCacheManager) manager(i)).shutdownAllCaches();
      }

      TestingUtil.killCacheManagers(this.cacheManagers);
      // Verify all nodes have state files
      verifyStateFilesExist();
      cacheManagers.clear();

      // Phase 2: Restart and create star partition
      createStatefulCacheManagers(false);

      // Create star partition BEFORE caches start
      starSplitCluster(0);

      LocalTopologyManager ltm = extractGlobalComponent(manager(0), LocalTopologyManager.class);

      // NOW start caches - joins happen through partitioned network
      // Midway through nodes joining, since the nodes are competing with suspect messages, the coordinator view changes.
      for (int i = 0; i < numMembersInCluster; i++) {
         cache(i, CACHE_NAME);

         // After some nodes joined the cache topology, the coordinator view updates and removes some nodes.
         if (i == numMembersInCluster - 3) {
            removeFirstNFrom(0, 2);
            // Wait until members are not included in the cache manager.
            eventually(() -> {
               List<Address> members = manager(0).getMembers();
               return !members.containsAll(List.of(manager(1).getAddress(), manager(2).getAddress()));
            });
            assertThat(ltm.getStableCacheTopology(CACHE_NAME)).isNull();
            assertThat(ltm.isCacheRebalancingEnabled(CACHE_NAME)).isFalse();
         }
      }

      // Since there was a partition during the restore, the stable topology should not be installed.
      // And state transfer should not be enabled.
      TestingUtil.join(ltm.stableTopologyCompletion(CACHE_NAME));
      assertThat(ltm.isCacheRebalancingEnabled(CACHE_NAME)).isTrue();

      // Phase 4: Heal partition and check for data loss
      healStarPartition();
      eventually(() -> cacheManagers.stream().allMatch(ecm -> ecm.getMembers().size() == numMembersInCluster));
      eventually(() -> cacheManagers.stream()
            .map(ecm -> extractGlobalComponent(ecm, LocalTopologyManager.class))
            .allMatch(localTopologyManager -> {
               CacheTopology topology = localTopologyManager.getStableCacheTopology(CACHE_NAME);
               return topology != null && topology.getMembers().size() == numMembersInCluster;
            })
      );

      // No data should be lost during the restart.
      int survivingData = cache(0, CACHE_NAME).size();
      assertThat(survivingData)
            .as("All data should survive graceful shutdown restart, even with partition")
            .isEqualTo(DATA_SIZE);
   }

   private void verifyStateFilesExist() {
      for (int i = 0; i < numMembersInCluster; i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration()
               .globalState().persistentLocation();
         java.io.File[] listFiles = new java.io.File(persistentLocation)
               .listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertThat(listFiles).hasSize(1);
      }
   }

   protected void starSplitCluster(int coordinator) {
      List<org.jgroups.Address> allMembers = channel(manager(0)).getView().getMembers();
      partitions = new Partition[numMembersInCluster - 1];

      JChannel c = channel(manager(coordinator));

      // Create N-1 partitions, each containing [coordinator, one other node]
      int partitionIndex = 0;
      for (int i = 0; i < numMembersInCluster; i++) {
         if (i == coordinator) continue;

         Partition p = new Partition(allMembers);
         p.addNode(c);
         p.addNode(channel(manager(i)));
         partitions[partitionIndex] = p;
         p.discardOtherMembersExceptCoordinator(coordinator);  // Only install DISCARD on non-coordinator
         partitionIndex++;
      }

      viewId.set((int) c.getView().getViewId().getId());
      int id = viewId.incrementAndGet();
      View v = View.create(c.getAddress(), id, c.view().getMembers().toArray(new org.jgroups.Address[0]));
      getGms(c).installView(v);

      // Install the new views
      for (Partition p : partitions) {
         p.partition(coordinator);
      }
   }

   protected void healStarPartition() {
      if (partitions == null || partitions.length == 0) {
         throw new IllegalStateException("No star partition to heal!");
      }

      // Enable for nodes to send messages to each while we install the new views.
      Partition p0 = partitions[0];
      for (Partition partition : partitions) {
         if (partition == p0) continue;
         p0.observeMembers(partition);
         partition.observeMembers(p0);
      }

      // Collect all unique channels from all partitions
      Set<JChannel> allChannelsSet = new HashSet<>();
      List<View> subviews = new ArrayList<>();

      for (Partition p : partitions) {
         allChannelsSet.addAll(p.channels());
         // Create subview for this partition for the merge
         List<org.jgroups.Address> partitionAddresses = p.channels().stream()
               .map(JChannel::getAddress)
               .toList();
         View subview = View.create(p.channels().get(0).getAddress(), viewId.get(),
               partitionAddresses.toArray(new org.jgroups.Address[0]));
         subviews.add(subview);
      }

      ArrayList<JChannel> allChannels = new ArrayList<>(allChannelsSet);

      for (JChannel channel : allChannels) {
         channel.getProtocolStack().removeProtocol(DISCARD.class);
      }

      // Use STABLE to clear sent messages (like in merge())
      for (JChannel c : allChannels) {
         STABLE stable = c.getProtocolStack().findProtocol(STABLE.class);
         stable.gc();
      }

      try {
         Thread.sleep(10);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }

      // Install merge view
      List<org.jgroups.Address> allAddresses = allChannels.stream()
            .map(JChannel::getAddress)
            .toList();
      MergeView mv = new MergeView(allChannels.get(0).getAddress(), viewId.incrementAndGet(), allAddresses, subviews);

      // Compute merge digest
      MutableDigest digest = new MutableDigest(allAddresses.toArray(new org.jgroups.Address[0]));
      for (JChannel c : allChannels) {
         digest.merge(getGms(c).getDigest());
      }

      // Re-enable discovery on all channels
      for (JChannel c : allChannels) {
         enableDiscoveryProtocol(c);
      }

      // Install the merge view on all channels
      for (JChannel c : allChannels) {
         getGms(c).installView(mv, digest);
      }

      // Clear partitions
      partitions = null;
   }

   protected void removeFirstNFrom(int target, int removals) {
      JChannel ch = channel(manager(target));
      org.jgroups.Address addr = ch.address();

      List<org.jgroups.Address> members = new ArrayList<>();
      members.add(addr);
      List<org.jgroups.Address> ignore = new ArrayList<>(ch.view().getMembers());

      int index = 0;
      for (org.jgroups.Address member : ch.getView().getMembers()) {
         if (Objects.equals(member, addr))
            continue;

         if (index++ < removals)
            continue;

         members.add(member);
      }

      ignore.removeAll(members);

      // Mark some nodes to be ignored by the coordinator.
      // Simulates as if the coordinator is unable to reach these nodes when they were removed from the view.
      DISCARD discard = new DISCARD();
      log.tracef("%s discarding messages from %s", ch.getAddress(), ignore);
      discard.excludeItself(false);
      for (org.jgroups.Address a : ignore) discard.addIgnoreMember(a);
      try {
         ch.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }

      // Install a new view without the removed nodes.
      int id = viewId.incrementAndGet();
      View suspectView = View.create(addr, id, members.toArray(new org.jgroups.Address[0]));
      getGms(ch).installView(suspectView);
      log.infof("Installing %s in %s", suspectView, ch.address());

      // Some operations in the server will assert the viewId to accept operations, like joining a topology.
      // We also update the view of all cluster members just bumping the IDs, the members are still the same.
      for (int i = 0; i < numMembersInCluster; i++) {
         JChannel c = channel(manager(i));
         if (ch == c)
            continue;

         View v = c.getView();
         View newView = View.create(v.getCoord(), id, v.getMembersRaw());
         log.infof("Installing %s in %s", newView, c.address());
         getGms(c).installView(newView);
      }
   }
}
