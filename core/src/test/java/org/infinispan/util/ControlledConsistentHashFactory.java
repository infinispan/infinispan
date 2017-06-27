package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.ScatteredConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.ClusterTopologyManager;

/**
 * ConsistentHashFactory implementation that allows the user to control who the owners are.
 *
* @author Dan Berindei
* @since 7.0
*/
public abstract class ControlledConsistentHashFactory<CH extends ConsistentHash> extends BaseControlledConsistentHashFactory<CH> {
   private volatile List<int[]> ownerIndexes;

   private volatile List<Address> membersToUse;

   /**
    * Create a consistent hash factory with a single segment.
    */
   public ControlledConsistentHashFactory(Trait<CH> trait, int primaryOwnerIndex, int... backupOwnerIndexes) {
      super(trait, 1);
      setOwnerIndexes(primaryOwnerIndex, backupOwnerIndexes);
   }

   /**
    * Create a consistent hash factory with multiple segments.
    */
   public ControlledConsistentHashFactory(Trait<CH> trait, int[] firstSegmentOwners, int[]... otherSegmentOwners) {
      super(trait, 1 + (otherSegmentOwners != null ? otherSegmentOwners.length : 0));
      setOwnerIndexes(firstSegmentOwners, otherSegmentOwners);
   }

   public void setOwnerIndexes(int primaryOwnerIndex, int... backupOwnerIndexes) {
      int[] firstSegmentOwners = concatOwners(primaryOwnerIndex, backupOwnerIndexes);
      setOwnerIndexes(firstSegmentOwners);
   }

   private int[] concatOwners(int primaryOwnerIndex, int[] backupOwnerIndexes) {
      int[] firstSegmentOwners;
      if (backupOwnerIndexes == null || backupOwnerIndexes.length == 0) {
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
      clusterTopologyManager.forceRebalance(cache.getName());
   }

   @Override
   protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
      int[] segmentOwnerIndexes = ownerIndexes.get(segmentIndex);
      List<Address> owners = new ArrayList<>(segmentOwnerIndexes.length);
      for (int index : segmentOwnerIndexes) {
         if (membersToUse != null) {
            Address owner = membersToUse.get(index);
            if (members.contains(owner)) {
               owners.add(owner);
            }
         }  else  if (index < members.size()) {
            owners.add(members.get(index));
         }
      }
      // A CH segment must always have at least one owner
      if (owners.isEmpty()) {
         owners.add(members.get(0));
      }
      return owners;
   }

   /**
    * @param membersToUse Owner indexes will be in this list, instead of the current list of members
    */
   public void setMembersToUse(List<Address> membersToUse) {
      this.membersToUse = membersToUse;
   }

   public static class Default extends ControlledConsistentHashFactory<DefaultConsistentHash> {
      public Default(int primaryOwnerIndex, int... backupOwnerIndexes) {
         super(new DefaultTrait(), primaryOwnerIndex, backupOwnerIndexes);
      }

      public Default(int[] firstSegmentOwners, int[]... otherSegmentOwners) {
         super(new DefaultTrait(), firstSegmentOwners, otherSegmentOwners);
      }
   }

   /**
    * Ignores backup-owner part of the calls
    */
   public static class Scattered extends ControlledConsistentHashFactory<ScatteredConsistentHash> {
      public Scattered(int primaryOwnerIndex) {
         super(new ScatteredTrait(), primaryOwnerIndex);
      }

      public Scattered(int[] segmentOwners) {
         super(new ScatteredTrait(), new int[] { segmentOwners[0] },
            Arrays.stream(segmentOwners, 1, segmentOwners.length).mapToObj(o -> new int[] { o }).toArray(int[][]::new));
      }

      @Override
      public void setOwnerIndexes(int primaryOwnerIndex, int... backupOwnerIndexes) {
         super.setOwnerIndexes(primaryOwnerIndex);
      }

      @Override
      public void setOwnerIndexes(int[] segment1Owners, int[]... otherSegmentOwners) {
         super.setOwnerIndexes(segment1Owners);
      }

      @Override
      public void setOwnerIndexesForSegment(int segmentIndex, int primaryOwnerIndex, int... backupOwnerIndexes) {
         super.setOwnerIndexesForSegment(segmentIndex, primaryOwnerIndex);
      }
   }
}
