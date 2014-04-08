package org.infinispan.util;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.ClusterTopologyManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.AssertJUnit.assertTrue;

/**
* ConsistentHashFactory implementation that has a single segment and allows the user to control who the owners are.
*
* @author Dan Berindei
* @since 7.0
*/
public class ControlledConsistentHashFactory extends SingleSegmentConsistentHashFactory {
   private volatile List<Integer> ownerIndexes;

   public ControlledConsistentHashFactory(int primaryOwnerIndex, int... backupOwnerIndexes) {
      setOwnerIndexes(primaryOwnerIndex, backupOwnerIndexes);
   }

   public void setOwnerIndexes(int primaryOwnerIndex, int... backupOwnerIndexes) {
      if (backupOwnerIndexes == null) {
         this.ownerIndexes = Arrays.asList(primaryOwnerIndex);
      } else {
         this.ownerIndexes = new ArrayList<Integer>(backupOwnerIndexes.length + 1);
         this.ownerIndexes.add(primaryOwnerIndex);
         for (int i = 0; i < backupOwnerIndexes.length; i++) {
            this.ownerIndexes.add(backupOwnerIndexes[i]);
         }
      }
   }

   public void triggerRebalance(Cache<?, ?> cache) throws Exception {
      EmbeddedCacheManager cacheManager = cache.getCacheManager();
      ClusterTopologyManager clusterTopologyManager = cacheManager
            .getGlobalComponentRegistry().getComponent(ClusterTopologyManager.class);
      assertTrue("triggerRebalance must be called on the coordinator node",
            cacheManager.getTransport().isCoordinator());
      clusterTopologyManager.triggerRebalance(cache.getName());
   }

   @Override
   protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners) {
      List<Address> owners = new ArrayList(ownerIndexes.size());
      for (int index : ownerIndexes) {
         if (index < members.size()) {
            owners.add(members.get(index));
         }
      }
      // A CH segment must always have at least one owner
      if (owners.isEmpty()) {
         owners.add(members.get(0));
      }
      return owners;
   }
}
