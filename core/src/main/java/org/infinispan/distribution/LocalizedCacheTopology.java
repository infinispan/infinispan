package org.infinispan.distribution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.util.ImmutableHopscotchHashSet;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.SingleSegmentKeyPartitioner;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

/**
 * Extends {@link CacheTopology} with information about keys owned by the local node.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class LocalizedCacheTopology extends CacheTopology {

   private final Address localAddress;
   private boolean connected;
   private final Set<Address> membersSet;
   private final KeyPartitioner keyPartitioner;
   private final boolean isDistributed;
   private final boolean allLocal;
   private final int numSegments;
   private final int maxOwners;
   private final DistributionInfo[] distributionInfos;

   /**
    * @param cacheMode Ignored, the result topology is always LOCAL
    * @param localAddress Address of the local node
    */
   public static LocalizedCacheTopology makeSingletonTopology(CacheMode cacheMode, Address localAddress) {
      List<Address> members = Collections.singletonList(localAddress);
       CacheTopology cacheTopology = new CacheTopology(-1, -1, null, null, Phase.NO_REBALANCE, members, null);
       return new LocalizedCacheTopology(CacheMode.LOCAL, cacheTopology, SingleSegmentKeyPartitioner.getInstance(),
             localAddress, false);
   }

   /**
    * Creates a new local topology that has a single address but multiple segments. This is useful when the data
    * storage is segmented in some way (ie. segmented store)
    * @param keyPartitioner partitioner to decide which segment a given key maps to
    * @param numSegments how many segments there are
    * @param localAddress the address of this node
    * @return segmented topology
    */
   public static LocalizedCacheTopology makeSegmentedSingletonTopology(KeyPartitioner keyPartitioner, int numSegments,
         Address localAddress) {
      return new LocalizedCacheTopology(keyPartitioner, numSegments, localAddress);
   }

   public LocalizedCacheTopology(CacheMode cacheMode, CacheTopology cacheTopology, KeyPartitioner keyPartitioner,
                                 Address localAddress, boolean connected) {
      super(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId(), cacheTopology.getCurrentCH(),
            cacheTopology.getPendingCH(), cacheTopology.getUnionCH(), cacheTopology.getPhase(), cacheTopology.getActualMembers(),
            cacheTopology.getMembersPersistentUUIDs());

      ConsistentHash readCH = getReadConsistentHash();
      ConsistentHash writeCH = getWriteConsistentHash();

      this.localAddress = localAddress;
      this.connected = connected;
      this.membersSet = new ImmutableHopscotchHashSet<>(cacheTopology.getMembers());
      this.keyPartitioner = keyPartitioner;
      this.isDistributed = cacheMode.isDistributed();
      boolean isReplicated = cacheMode.isReplicated();
      boolean isInvalidation = cacheMode.isInvalidation();

      if (isDistributed) {
         this.numSegments = readCH.getNumSegments();
         this.distributionInfos = new DistributionInfo[numSegments];
         int maxOwners = 1;
         for (int segmentId = 0; segmentId < numSegments; segmentId++) {
            Address primary = readCH.locatePrimaryOwnerForSegment(segmentId);
            List<Address> readOwners = readCH.locateOwnersForSegment(segmentId);
            List<Address> writeOwners = writeCH.locateOwnersForSegment(segmentId);
            Collection<Address> writeBackups = writeOwners.subList(1, writeOwners.size());
            this.distributionInfos[segmentId] =
                  new DistributionInfo(segmentId, primary, readOwners, writeOwners, writeBackups, localAddress);
            maxOwners = Math.max(maxOwners, writeOwners.size());
         }
         this.maxOwners = maxOwners;
         this.allLocal = false;
      } else if (isReplicated || isInvalidation) {
         this.numSegments = readCH.getNumSegments();
         // Writes/invalidations must be broadcast to the entire cluster
         Map<Address, List<Address>> readOwnersMap = new HashMap<>();
         Map<Address, List<Address>> writeOwnersMap = new HashMap<>();
         this.distributionInfos = new DistributionInfo[numSegments];
         for (int segmentId = 0; segmentId < numSegments; segmentId++) {
            int segmentCopy = segmentId;
            Address primary = readCH.locatePrimaryOwnerForSegment(segmentId);
            List<Address> readOwners = readOwnersMap.computeIfAbsent(primary, p ->
                  Immutables.immutableListCopy(readCH.locateOwnersForSegment(segmentCopy)));
            List<Address> writeOwners = writeOwnersMap.computeIfAbsent(primary, p ->
                  Immutables.immutableListCopy(writeCH.locateOwnersForSegment(segmentCopy)));
            List<Address> writeBackups = writeOwners.subList(1, writeOwners.size());
            this.distributionInfos[segmentId] =
               new DistributionInfo(segmentId, primary, readOwners, writeOwners, writeBackups, localAddress);
         }
         this.maxOwners = cacheTopology.getMembers().size();
         this.allLocal = readOwnersMap.containsKey(localAddress);
      } else {
         assert cacheMode == CacheMode.LOCAL;
         this.numSegments = 1;
         List<Address> owners = Collections.singletonList(localAddress);
         List<Address> writeBackups = Collections.emptyList();
         this.distributionInfos = new DistributionInfo[]{
               new DistributionInfo(0, localAddress, owners, owners, writeBackups, localAddress)
         };
         this.maxOwners = 1;
         this.allLocal = true;
      }
   }

   private LocalizedCacheTopology(KeyPartitioner keyPartitioner, int numSegments, Address localAddress) {
      super(-1, -1, null, null, null, Collections.singletonList(localAddress), null);
      this.localAddress = localAddress;
      this.numSegments = numSegments;
      this.keyPartitioner = keyPartitioner;
      this.membersSet = Collections.singleton(localAddress);
      this.isDistributed = false;
      // Reads and writes are local, only the invalidation is replicated
      List<Address> owners = Collections.singletonList(localAddress);
      this.distributionInfos = new DistributionInfo[numSegments];
      for (int i = 0; i < distributionInfos.length; ++i) {
         distributionInfos[i] = new DistributionInfo(i, localAddress, owners, owners, Collections.emptyList(), localAddress);
      }
      this.maxOwners = 1;
      this.allLocal = true;
   }

   /**
    * @return {@code true} iff key {@code key} can be read without going remote.
    */
   public boolean isReadOwner(Object key) {
      if (allLocal)
         return true;

      int segmentId = keyPartitioner.getSegment(key);
      return distributionInfos[segmentId].isReadOwner();
   }

   public boolean isSegmentReadOwner(int segment) {
      return allLocal || distributionInfos[segment].isReadOwner();
   }


   /**
    * @return {@code true} iff writing a value for key {@code key} will update it on the local node.
    */
   public boolean isWriteOwner(Object key) {
      if (allLocal)
         return true;

      int segmentId = keyPartitioner.getSegment(key);
      return distributionInfos[segmentId].isWriteOwner();
   }

   public boolean isSegmentWriteOwner(int segment) {
      return allLocal || distributionInfos[segment].isWriteOwner();
   }

   /**
    * @return The consistent hash segment of key {@code key}
    */
   public int getSegment(Object key) {
      return keyPartitioner.getSegment(key);
   }

   /**
    * @return Information about the ownership of segment {@code segment}, including the primary owner.
    * @deprecated since 9.3 please use {@link #getSegmentDistribution(int)} instead.
    */
   @Deprecated
   public DistributionInfo getDistributionForSegment(int segmentId) {
      return getSegmentDistribution(segmentId);
   }

   public DistributionInfo getSegmentDistribution(int segmentId) {
      return distributionInfos[segmentId];
   }

   /**
    * @return Information about the ownership of key {@code key}, including the primary owner.
    */
   public DistributionInfo getDistribution(Object key) {
      int segmentId = keyPartitioner.getSegment(key);
      return distributionInfos[segmentId];
   }

   /**
    * @return An unordered collection with the write owners of {@code key}.
    */
   public Collection<Address> getWriteOwners(Object key) {
      int segmentId = isDistributed ? keyPartitioner.getSegment(key) : 0;
      return distributionInfos[segmentId].writeOwners();
   }

   /**
    * @return An unordered collection with the write owners of {@code keys}.
    */
   public Collection<Address> getWriteOwners(Collection<?> keys) {
      if (keys.isEmpty()) {
         return Collections.emptySet();
      }
      if (isDistributed) {
         if (keys.size() == 1) {
            Object singleKey = keys.iterator().next();
            return getDistribution(singleKey).writeOwners();
         } else {
            IntSet segments = IntSets.mutableEmptySet(numSegments);
            // Expecting some overlap between keys
            Set<Address> owners = new HashSet<>(2 * maxOwners);
            for (Object key : keys) {
               int segment = keyPartitioner.getSegment(key);
               if (segments.add(segment)) {
                  owners.addAll(getSegmentDistribution(segment).writeOwners());
               }
            }
            return owners;
         }
      } else {
         return getSegmentDistribution(0).writeOwners();
      }
   }

   /**
    * @return The segments owned by the local node for reading.
    */
   public IntSet getLocalReadSegments() {
      if (isDistributed) {
         IntSet localSegments = IntSets.mutableEmptySet(numSegments);
         for (int segment = 0; segment < numSegments; segment++) {
            if (distributionInfos[segment].isReadOwner()) {
               localSegments.set(segment);
            }
         }
         return localSegments;
      } else if (allLocal) {
         return IntSets.immutableRangeSet(numSegments);
      } else {
         return IntSets.immutableEmptySet();
      }
   }

   /**
    * @return The segments owned by the local node for writing.
    */
   public IntSet getLocalWriteSegments() {
      if (isDistributed) {
         IntSet localSegments = IntSets.mutableEmptySet(numSegments);
         for (int segmentId = 0; segmentId < numSegments; segmentId++) {
            if (distributionInfos[segmentId].isWriteOwner()) {
               localSegments.set(segmentId);
            }
         }
         return localSegments;
      } else if (allLocal) {
         return IntSets.immutableRangeSet(numSegments);
      } else {
         return IntSets.immutableEmptySet();
      }
   }

   /**
    * @return The segments owned by the local node as primary owner.
    */
   public IntSet getLocalPrimarySegments() {
      if (membersSet.size() > 1) {
         IntSet localSegments = IntSets.mutableEmptySet(numSegments);
         for (int segment = 0; segment < numSegments; segment++) {
            if (distributionInfos[segment].isPrimary()) {
               localSegments.set(segment);
            }
         }
         return localSegments;
      } else {
         return IntSets.immutableRangeSet(numSegments);
      }
   }
   /**
    * @return The number of segments owned by the local node for writing.
    */
   public int getLocalWriteSegmentsCount() {
      if (isDistributed) {
         int count = 0;
         for (int segment = 0; segment < numSegments; segment++) {
            if (distributionInfos[segment].isWriteOwner()) {
               count++;
            }
         }
         return count;
      } else if (allLocal) {
         return numSegments;
      } else {
         return 0;
      }
   }

   /**
    * @return The address of the local node.
    */
   public Address getLocalAddress() {
      return localAddress;
   }

   public Set<Address> getMembersSet() {
      return membersSet;
   }

   /**
    * @return {@code true} if the local node received this topology from the coordinator,
    * {@code false} otherwise (e.g. during preload).
    */
   public boolean isConnected() {
      return connected;
   }

   public int getNumSegments() {
      return numSegments;
   }
}
