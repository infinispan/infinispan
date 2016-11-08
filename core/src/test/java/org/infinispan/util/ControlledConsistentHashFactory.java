package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.ClusterTopologyManager;

/**
 * ConsistentHashFactory implementation that allows the user to control who the owners are.
 *
* @author Dan Berindei
* @since 7.0
*/
@SerializeWith(ControlledConsistentHashFactory.Ext.class)
public class ControlledConsistentHashFactory extends BaseControlledConsistentHashFactory {
   private volatile List<int[]> ownerIndexes;

   private volatile List<Address> membersToUse;

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

   public ControlledConsistentHashFactory(int numSegments, List<int[]> ownerIndexes, List<Address> membersToUse) {
      super(numSegments);
      this.ownerIndexes = ownerIndexes;
      this.membersToUse = membersToUse;
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

   public static final class Ext implements Externalizer<ControlledConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, ControlledConsistentHashFactory object) throws IOException {
         output.writeInt(object.numSegments);
         MarshallUtil.marshallCollection(object.ownerIndexes, output);
         MarshallUtil.marshallCollection(object.membersToUse, output);
      }

      @Override
      public ControlledConsistentHashFactory readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int numSegments = input.readInt();
         List<int[]> ownerIndexes = MarshallUtil.unmarshallCollection(input, ArrayList::new);
         List<Address> membersToUse = MarshallUtil.unmarshallCollection(input, ArrayList::new);
         return new ControlledConsistentHashFactory(numSegments, ownerIndexes, membersToUse);
      }

   }

}
