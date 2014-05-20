package org.infinispan.util;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.ClusterTopologyManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * ConsistentHashFactory implementation that allows the user to control who the owners are.
 *
* @author Dan Berindei
* @since 7.0
*/
public class ControlledConsistentHashFactory extends BaseControlledConsistentHashFactory {
   private volatile List<int[]> ownerIndexes;

   /**
    * Create a consistent hash factory with a single segment.
    */
   public ControlledConsistentHashFactory(int primaryOwnerIndex, int... backupOwnerIndexes) {
      super(1);
      setOwnerIndexes(primaryOwnerIndex, backupOwnerIndexes);
   }

   /**
    * Create a consistent hash factory with multiple segments.
    */
   public ControlledConsistentHashFactory(int[] firstSegmentOwners, int[]... otherSegmentOwners) {
      super(1 + (otherSegmentOwners != null ? otherSegmentOwners.length : 0));
      setOwnerIndexes(firstSegmentOwners, otherSegmentOwners);
   }

   public void setOwnerIndexes(int primaryOwnerIndex, int... backupOwnerIndexes) {
      int[] firstSegmentOwners = concatOwners(primaryOwnerIndex, backupOwnerIndexes);
      setOwnerIndexes(firstSegmentOwners);
   }

   private int[] concatOwners(int primaryOwnerIndex, int[] backupOwnerIndexes) {
      int[] firstSegmentOwners;
      if (backupOwnerIndexes == null) {
         firstSegmentOwners = new int[]{primaryOwnerIndex};
      } else {
         firstSegmentOwners = new int[backupOwnerIndexes.length + 1];
         firstSegmentOwners[0] = primaryOwnerIndex;
         for (int i = 0; i < backupOwnerIndexes.length; i++) {
            firstSegmentOwners[i + 1] = backupOwnerIndexes[i];
         }
      }
      return firstSegmentOwners;
   }

   public void setOwnerIndexes(int[] segment1Owners, int[]... otherSegmentOwners) {
      ArrayList<int[]> newOwnerIndexes = new ArrayList<int[]>(numSegments);
      newOwnerIndexes.add(segment1Owners);
      if (otherSegmentOwners != null) {
         newOwnerIndexes.addAll(Arrays.asList(otherSegmentOwners));
      }
      assertEquals(numSegments, newOwnerIndexes.size());
      this.ownerIndexes = newOwnerIndexes;
   }

   public void setOwnerIndexesForSegment(int segmentIndex, int primaryOwnerIndex, int... backupOwnerIndexes) {
      ArrayList<int[]> newOwnerIndexes = new ArrayList<int[]>(ownerIndexes);
      newOwnerIndexes.set(segmentIndex, concatOwners(primaryOwnerIndex, backupOwnerIndexes));
      this.ownerIndexes = newOwnerIndexes;
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
   protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
      int[] segmentOwnerIndexes = ownerIndexes.get(segmentIndex);
      List<Address> owners = new ArrayList(segmentOwnerIndexes.length);
      for (int index : segmentOwnerIndexes) {
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
