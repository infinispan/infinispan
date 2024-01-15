package org.infinispan.partitionhandling;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.PersistentUUID;
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

   public void testCrashBeforeRecover() throws Exception {
      Map<JGroupsAddress, PersistentUUID> addressMappings = createInitialCluster();

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
            .collect(Collectors.toList());
      LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      eventually(() -> {
         List<Address> actual = ltm.getCacheTopology(CACHE_NAME).getActualMembers();
         return actual.size() == members.size() && actual.containsAll(members);
      });

      // The cluster will never be complete wrt data before the restarts.
      // The nodes that restart will clear the underlying store, as it could lead to inconsistency.
      // Right now, what it is possible to check is that the restart is successful and the remaining data.
      // With M owners and N nodes, we still have left around ~M/N of data.
      checkClusterRestartedCorrectly(addressMappings);
      assertThat(cache(0, CACHE_NAME).size())
            .isBetween((int) (((float) numberOfOwners / numMembersInCluster) * DATA_SIZE), DATA_SIZE);
   }

   private void killManagers1and2() throws Exception {
      EmbeddedCacheManager e2 = cacheManagers.remove(2);
      EmbeddedCacheManager e1 = cacheManagers.remove(1);

      e2.start();
      Future<Void> f2 = fork(e2::stop);
      Future<Void> f1 = fork(e1::stop);

      LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      eventually(() -> ltm.getCacheAvailability(CACHE_NAME) == AvailabilityMode.DEGRADED_MODE);
      eventually(() -> ltm.getCacheTopology(CACHE_NAME).getActualMembers().size() == 1);

      f2.get(10, TimeUnit.SECONDS);
      f1.get(10, TimeUnit.SECONDS);
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
