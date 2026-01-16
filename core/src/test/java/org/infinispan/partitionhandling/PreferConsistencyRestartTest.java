package org.infinispan.partitionhandling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.partitionhandling.AvailabilityMode.AVAILABLE;
import static org.infinispan.partitionhandling.AvailabilityMode.DEGRADED_MODE;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.testing.Exceptions;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.LocalTopologyManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.PreferConsistencyRestartTest")
public class PreferConsistencyRestartTest extends BaseStatefulPartitionHandlingTest {

   {
      lockingMode = null;
      partitionHandling = PartitionHandling.DENY_READ_WRITES;
      cacheMode = CacheMode.DIST_SYNC;
      numberOfOwners = 2;
      numMembersInCluster = 3;
      createDefault = true;
   }

   public void testOnlyFreshNodeLeftDuringDegraded() throws Exception {
      var addressMappings = createInitialCluster();

      checkData();

      // Stop two nodes, one of them is the coordinator.
      stopManagers(2, 0);

      // The remaining node is the new coordinator.
      assertThat(manager(0).isCoordinator()).isTrue();
      eventuallyClusterTopologyCoordinator(0);

      // The cache is now in degraded mode.
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      assertThat(ctm.getAvailabilityMode(CACHE_NAME)).isEqualTo(DEGRADED_MODE);

      // Add new node, but it is still degraded.
      createStatefulCacheManager("Z", true);
      manager(1).getCache(CACHE_NAME);
      assertThat(ctm.getAvailabilityMode(CACHE_NAME)).isEqualTo(DEGRADED_MODE);

      // The coordinator leaves, and only the extraneous node remains.
      stopManagers(0);
      assertThat(manager(0).isCoordinator()).isTrue();

      // Cache still degraded.
      ctm = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      eventuallyClusterTopologyCoordinator(0);
      assertThat(ctm.getAvailabilityMode(CACHE_NAME)).isEqualTo(DEGRADED_MODE);

      // The previous nodes join now.
      createStatefulCacheManager("A", false);
      createStatefulCacheManager("B", false);
      createStatefulCacheManager("C", false);

      // All nodes join.
      for (EmbeddedCacheManager manager : managers()) {
         manager.getCache(CACHE_NAME);
      }

      // After all previous members join, the cache is available.
      assertThat(ctm.getAvailabilityMode(CACHE_NAME)).isEqualTo(AVAILABLE);

      // Check restart. Add new extraneous node.
      var uuid = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class)
            .getPersistentUUID();
      addressMappings.put(manager(0).getAddress(), uuid);
      checkPersistentUUIDMatch(addressMappings);
   }

   public void testCompletelyNewClusterWhileDegraded() {
      createInitialCluster();

      checkData();

      // Stop two nodes, one of them is the coordinator.
      stopManagers(2, 0);

      // The remaining node is the new coordinator.
      assertThat(manager(0).isCoordinator()).isTrue();
      eventuallyClusterTopologyCoordinator(0);

      // The cache is now in degraded mode.
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      assertThat(ctm.getAvailabilityMode(CACHE_NAME)).isEqualTo(DEGRADED_MODE);

      // Add new node, but it is still degraded.
      createStatefulCacheManager("Z", true);
      manager(1).getCache(CACHE_NAME);
      assertThat(ctm.getAvailabilityMode(CACHE_NAME)).isEqualTo(DEGRADED_MODE);

      // The coordinator leaves, and only the extraneous node remains.
      stopManagers(0);
      assertThat(manager(0).isCoordinator()).isTrue();

      // Cache still degraded.
      ctm = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      eventuallyClusterTopologyCoordinator(0);
      assertThat(ctm.getAvailabilityMode(CACHE_NAME)).isEqualTo(DEGRADED_MODE);

      // Add extraneous nodes.
      createStatefulCacheManager("Y", true);
      createStatefulCacheManager("X", true);

      for (EmbeddedCacheManager manager : managers()) {
         manager.getCache(CACHE_NAME);
      }

      // Since we only have new members, the cluster will remain DEGRADED.
      // It will require a manual intervention.
      assertThat(ctm.getAvailabilityMode(CACHE_NAME)).isEqualTo(DEGRADED_MODE);

      // Force availability of cluster.
      await(ctm.forceAvailabilityMode(CACHE_NAME, AVAILABLE));
      assertThat(ctm.getAvailabilityMode(CACHE_NAME)).isEqualTo(AVAILABLE);

      // Again, since the cluster is completely new, all data is gone.
      assertThat(cache(0, CACHE_NAME).size()).isZero();

      // Verify cache accept operations.
      for (int i = 0; i < 100; i++) {
         cache(0, CACHE_NAME).put(String.valueOf(i), String.valueOf(i));
      }
   }

   public void testCoordinatorChangesWhileDegraded() throws Exception {
      var addressMappings = createInitialCluster();

      // Operate directly on the default cache.
      // Since it is created by default, it could cause the node fail to start.
      String defaultCacheName = "defaultcache";

      checkData();

      // Stop two nodes, one of them is the coordinator.
      stopManagers(2, 0);

      // The remaining node is the new coordinator.
      assertThat(manager(0).isCoordinator()).isTrue();

      // The cache is now in degraded mode.
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      eventuallyClusterTopologyCoordinator(0);
      assertThat(ctm.getAvailabilityMode(defaultCacheName)).isEqualTo(DEGRADED_MODE);

      // Previous coordinator joins.
      createStatefulCacheManager("A", false);

      // Still in degraded.
      assertThat(ctm.getAvailabilityMode(defaultCacheName)).isEqualTo(DEGRADED_MODE);

      // The current coordinator leaves, yield control to previous coordinator.
      stopManagers(0);
      assertThat(manager(0).isCoordinator()).isTrue();

      // And the cache is still in degraded mode.
      ctm = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      eventuallyClusterTopologyCoordinator(0);
      assertThat(ctm.getAvailabilityMode(defaultCacheName)).isEqualTo(DEGRADED_MODE);

      // Add all members back.
      createStatefulCacheManager("B", false);
      assertThat(ctm.getAvailabilityMode(defaultCacheName)).isEqualTo(DEGRADED_MODE);

      createStatefulCacheManager("C", false);
      assertThat(ctm.getAvailabilityMode(defaultCacheName)).isEqualTo(AVAILABLE);

      // The UUID mapping recovered successfully.
      checkPersistentUUIDMatch(addressMappings);
   }

   private void eventuallyClusterTopologyCoordinator(int index) {
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager(index), ClusterTopologyManager.class);
      eventually(() -> ctm.getStatus() == ClusterTopologyManager.ClusterManagerStatus.COORDINATOR);
   }

   public void testCrashBeforeRecover() throws Exception {
      var addressMappings = createInitialCluster();

      checkData();

      // Create the key before shut down.
      MagicKey mkOther = new MagicKey("kc", cache(1, CACHE_NAME), cache(2, CACHE_NAME));

      killManagers1and2();

      Exceptions.expectException(AvailabilityException.class,
            "ISPN000306: Key '.*' is not available. Not all owners are in this partition",
            () -> cache(0, CACHE_NAME).put(mkOther, "fail"));

      // If state is cleared, the cluster will never form again.
      createStatefulCacheManager(Character.toString('B'), false);
      createStatefulCacheManager(Character.toString('C'), false);

      // Create default cache.
      waitForClusterToForm();

      // Check still DEGRADED in the other.
      // We didn't start the cache on the new nodes.
      Exceptions.expectException(AvailabilityException.class,
            "ISPN000306: Key '.*' is not available. Not all owners are in this partition",
            () -> cache(0, CACHE_NAME).put(mkOther, "fail"));

      // Insert the new key in the default, which should not be degraded.
      MagicKey mkDefault = new MagicKey("kd", cache(1), cache(2));
      cache(0).put(mkDefault, "value");
      assertThat(cache(0).get(mkDefault)).isEqualTo("value");

      // Stop the cache managers and recreate to restart for the other cache.
      killManagers1and2();

      createStatefulCacheManager(Character.toString('B'), false);
      createStatefulCacheManager(Character.toString('C'), false);

      // Reconnect, create first default and then other.
      waitForClusterToForm();

      // Default still has the inserted key.
      if (isASegmentOwner(mkDefault.getSegment())) {
         // Segment might have change and A could be an owner of the segment (primary or not).
         // That would cause a local read, which returns null. In this case, we do a local read with either B or C.
         assertThat(readKeyLocallyBOrC(mkDefault)).isEqualTo("value");
      } else {
         // If A is not the owner, we can execute the command from A, going to the remote nodes.
         assertThat(cache(0).get(mkDefault)).isEqualTo("value");
      }

      // Now recreates the other. Should be created and restored successfully!
      waitForClusterToForm(CACHE_NAME);
      List<Address> members = cacheManagers.stream()
            .map(EmbeddedCacheManager::getAddress)
            .toList();
      LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      eventually(() -> {
         List<Address> actual = ltm.getCacheTopology(CACHE_NAME).getActualMembers();
         return actual.size() == members.size() && actual.containsAll(members);
      });

      // The cluster will never be complete wrt data before the restarts.
      // The nodes that restart will clear the underlying store, as it could lead to inconsistency.
      // Right now, what it is possible to check is that the restart is successful and the remaining data.
      // With M owners and N nodes, we still have left around ~M/N of data.
      checkPersistentUUIDMatch(addressMappings);
      assertThat(cache(0, CACHE_NAME).size())
            .isBetween((int) (((float) numberOfOwners / numMembersInCluster) * DATA_SIZE), DATA_SIZE);
   }

   private void killManagers1and2() {
      stopManagers(2, 1);

      LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      eventually(() -> ltm.getCacheAvailability(CACHE_NAME) == DEGRADED_MODE);
      eventually(() -> ltm.getCacheTopology(CACHE_NAME).getActualMembers().size() == 1);
   }

   private void stopManagers(int ... indexes) {
      List<Future<Void>> stops = new ArrayList<>(indexes.length);
      for (int index : indexes) {
         EmbeddedCacheManager ecm = cacheManagers.remove(index);
         stops.add(fork(ecm::stop));
      }

      stops.forEach(this::join);
   }

   private void join(Future<Void> future) {
      try {
         future.get(10, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
         throw new RuntimeException(e);
      }
   }

   private boolean isASegmentOwner(int segment) {
      LocalizedCacheTopology cacheTopology = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology();
      return cacheTopology.getSegmentDistribution(segment).isPrimary()
         || cacheTopology.getSegmentDistribution(segment).isReadOwner();
   }

   private Object readKeyLocallyBOrC(MagicKey mk) {
      Object v = cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).get(mk);
      return v == null
            ? cache(2).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).get(mk)
            : v;
   }
}
