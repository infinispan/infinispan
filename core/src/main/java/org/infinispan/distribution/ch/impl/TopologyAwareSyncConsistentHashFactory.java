package org.infinispan.distribution.ch.impl;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.topologyaware.TopologyInfo;
import org.infinispan.distribution.topologyaware.TopologyLevel;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;

/**
 * A {@link org.infinispan.distribution.ch.ConsistentHashFactory} implementation that guarantees caches
 * with the same members have the same consistent hash and also tries to distribute segments based on the
 * topology information in {@link org.infinispan.configuration.global.TransportConfiguration}.
 * <p/>
 * It has a drawback compared to {@link org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory}:
 * it can potentially move a lot more segments during a rebalance than strictly necessary.
 * <p/>
 * It is not recommended using the {@code TopologyAwareSyncConsistentHashFactory} with a very small number
 * of segments. The distribution of segments to owners gets better with a higher number of segments, and is
 * especially bad when {@code numSegments &lt; numNodes}
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class TopologyAwareSyncConsistentHashFactory extends SyncConsistentHashFactory {
   @Override
   protected Builder createBuilder(Hash hashFunction, int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
      return new Builder(hashFunction, numOwners, numSegments, members, capacityFactors);
   }

   protected static class Builder extends SyncConsistentHashFactory.Builder {
      protected final TopologyInfo topologyInfo;

      protected TopologyLevel currentLevel = TopologyLevel.SITE;

      protected Builder(Hash hashFunction, int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
         super(hashFunction, numOwners, numSegments, members, capacityFactors);
         topologyInfo = new TopologyInfo(members, capacityFactors);
      }

      @Override
      protected void copyOwners() {
         copyOwnersForLevel(TopologyLevel.SITE);
         copyOwnersForLevel(TopologyLevel.RACK);
         copyOwnersForLevel(TopologyLevel.MACHINE);
         copyOwnersForLevel(TopologyLevel.NODE);
      }

      private void copyOwnersForLevel(TopologyLevel topologyLevel) {
         currentLevel = topologyLevel;
         ignoreMaxSegments = false;
         super.doCopyOwners();
         ignoreMaxSegments = true;
         super.doCopyOwners();
      }

      @Override
      protected void addOwner(int segment, Address candidate) {
         List<Address> owners = segmentOwners[segment];
         if (owners.size() < actualNumOwners && !locationAlreadyAdded(candidate, owners, currentLevel)) {
            if (!ignoreMaxSegments) {
               if (owners.isEmpty()) {
                  long maxSegments = Math.round(computeExpectedSegmentsForNode(candidate, 1) * PRIMARY_SEGMENTS_ALLOWED_VARIATION);
                  if (stats.getPrimaryOwned(candidate) < maxSegments) {
                     addOwnerNoCheck(segment, candidate);
                  }
               } else {
                  long maxSegments = Math.round(computeExpectedSegmentsForNode(candidate, actualNumOwners) * OWNED_SEGMENTS_ALLOWED_VARIATION);
                  if (stats.getOwned(candidate) < maxSegments) {
                     addOwnerNoCheck(segment, candidate);
                  }
               }
            } else {
               if (!capacityFactors.get(candidate).equals(0f)) {
                  addOwnerNoCheck(segment, candidate);
               }
            }
         }
      }

      @Override
      protected boolean canAddOwners(List<Address> owners) {
         return owners.size() < topologyInfo.getDistinctLocationsCount(currentLevel, actualNumOwners);
      }

      @Override
      protected double computeExpectedSegmentsForNode(Address node, int numCopies) {
         return topologyInfo.computeExpectedSegments(numSegments, numCopies, node);
      }

      private boolean locationAlreadyAdded(Address candidate, List<Address> owners, TopologyLevel level) {
         TopologyAwareAddress topologyAwareCandidate = (TopologyAwareAddress) candidate;
         boolean locationAlreadyAdded = false;
         for (Address owner : owners) {
            TopologyAwareAddress topologyAwareOwner = (TopologyAwareAddress) owner;
            switch (level) {
               case SITE:
                  locationAlreadyAdded = topologyAwareCandidate.isSameSite(topologyAwareOwner);
                  break;
               case RACK:
                  locationAlreadyAdded = topologyAwareCandidate.isSameRack(topologyAwareOwner);
                  break;
               case MACHINE:
                  locationAlreadyAdded = topologyAwareCandidate.isSameMachine(topologyAwareOwner);
                  break;
               case NODE:
                  locationAlreadyAdded = owner.equals(candidate);
            }
            if (locationAlreadyAdded)
               break;
         }
         return locationAlreadyAdded;
      }

   }

   public static class Externalizer extends AbstractExternalizer<TopologyAwareSyncConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, TopologyAwareSyncConsistentHashFactory chf) {
      }

      @Override
      @SuppressWarnings("unchecked")
      public TopologyAwareSyncConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new TopologyAwareSyncConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.TOPOLOGY_AWARE_SYNC_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends TopologyAwareSyncConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends TopologyAwareSyncConsistentHashFactory>>singleton(TopologyAwareSyncConsistentHashFactory.class);
      }
   }
}
