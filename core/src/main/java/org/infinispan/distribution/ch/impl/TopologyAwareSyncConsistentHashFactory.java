package org.infinispan.distribution.ch.impl;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.distribution.topologyaware.TopologyInfo;
import org.infinispan.distribution.topologyaware.TopologyLevel;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

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

      protected TopologyLevel currentLevel;

      protected Builder(Hash hashFunction, int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
         super(hashFunction, numOwners, numSegments, members, capacityFactors);
         // Use the processed capacity factors and numOwners
         topologyInfo = new TopologyInfo(numSegments, this.actualNumOwners, members, this.capacityFactors);
         currentLevel = TopologyLevel.SITE;
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
      protected boolean addBackupOwner(int segment, Address candidate) {
         if (capacityFactors.get(candidate).equals(0f))
            return false;

         List<Address> owners = segmentOwners[segment];
         if (owners.size() < actualNumOwners && !topologyInfo.duplicateLocation(currentLevel, owners, candidate, false)) {
            if (!ignoreMaxSegments) {
               if (owners.isEmpty()) {
                  long maxSegments = Math.round(getExpectedPrimarySegments(candidate) * PRIMARY_SEGMENTS_ALLOWED_VARIATION);
                  if (stats.getPrimaryOwned(candidate) < maxSegments) {
                     addOwnerNoCheck(segment, candidate);
                     return true;
                  }
               } else {
                  long maxSegments = Math.round(getExpectedOwnedSegments(candidate) * OWNED_SEGMENTS_ALLOWED_VARIATION);
                  if (stats.getOwned(candidate) < maxSegments) {
                     addOwnerNoCheck(segment, candidate);
                     return true;
                  }
               }
            } else {
               addOwnerNoCheck(segment, candidate);
               return true;
            }
         }
         return false;
      }

      @Override
      protected boolean canAddOwners(List<Address> owners) {
         return owners.size() < actualNumOwners &&
                owners.size() < topologyInfo.getDistinctLocationsCount(currentLevel);
      }

      @Override
      protected void populateExtraOwners(int numSegments) {
         currentLevel = TopologyLevel.SITE;
         super.populateExtraOwners(numSegments);
         currentLevel = TopologyLevel.RACK;
         super.populateExtraOwners(numSegments);
         currentLevel = TopologyLevel.MACHINE;
         super.populateExtraOwners(numSegments);
         currentLevel = TopologyLevel.NODE;
         super.populateExtraOwners(numSegments);

      }

      @Override
      protected double getExpectedPrimarySegments(Address node) {
         return topologyInfo.getExpectedPrimarySegments(node);
      }

      @Override
      protected double getExpectedOwnedSegments(Address node) {
         return topologyInfo.getExpectedOwnedSegments(node);
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
