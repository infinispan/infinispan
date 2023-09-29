package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.PersistentUUID;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "globalstate.ThreeNodeDegradedGlobalStateTest")
public class ThreeNodeDegradedGlobalStateTest extends AbstractGlobalStateRestartTest {

   @Override
   protected int getClusterSize() {
      return 3;
   }

   @Override
   protected void applyCacheManagerClusteringConfiguration(ConfigurationBuilder config) {
      config.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2);
      config.clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);
   }

   public void testMajorityOfClusterCrashes() throws Throwable {
      Map<JGroupsAddress, PersistentUUID> addressMappings = createInitialCluster();

      checkData();

      // Create the key before shut down.
      MagicKey mk = new MagicKey("key", cache(1, CACHE_NAME), cache(2, CACHE_NAME));

      EmbeddedCacheManager e2 = cacheManagers.remove(2);
      EmbeddedCacheManager e1 = cacheManagers.remove(1);
      Future<Void> f2 = fork(() -> TestingUtil.killCacheManagers(e2));
      Future<Void> f1 = fork(() -> TestingUtil.killCacheManagers(e1));

      LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      eventually(() -> ltm.getCacheAvailability(CACHE_NAME) == AvailabilityMode.DEGRADED_MODE);
      eventually(() -> ltm.getCacheTopology(CACHE_NAME).getActualMembers().size() == 1);

      f2.get(10, TimeUnit.SECONDS);
      f1.get(10, TimeUnit.SECONDS);

      Exceptions.expectException(AvailabilityException.class,
            "ISPN000306: Key '.*' is not available. Not all owners are in this partition",
            () -> cache(0, CACHE_NAME).put(mk, "fail"));

      // If state is cleared, the cluster will never form again.
      createStatefulCacheManager(Character.toString('B'), false);
      createStatefulCacheManager(Character.toString('C'), false);
      waitForClusterToForm(CACHE_NAME);

      List<Address> members = cacheManagers.stream()
            .map(EmbeddedCacheManager::getAddress)
            .collect(Collectors.toList());
      eventually(() -> {
         List<Address> actual = ltm.getCacheTopology(CACHE_NAME).getActualMembers();
         return actual.size() == members.size() && actual.containsAll(members);
      });

      // The cluster will never be complete wrt data before the restarts.
      // The nodes that restart will clear the underlying store, as it could lead to inconsistency.
      // Right now, what it is possible to check is that the restart is successful and the remaining data.
      // With 2 owners and N nodes, we still have left around ~2/N of data.
      checkClusterRestartedCorrectly(addressMappings);
      assertThat(cache(0, CACHE_NAME).size())
            .isBetween((int) ((2. / getClusterSize()) * DATA_SIZE), DATA_SIZE);
   }
}
