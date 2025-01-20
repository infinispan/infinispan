package org.infinispan.topology;

import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * This class contains the information that a cache needs to supply to the coordinator when starting up.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_JOIN_INFO)
public class CacheJoinInfo {
   // Global configuration
   private final ConsistentHashFactory<?> consistentHashFactory;
   private final int numSegments;
   private final int numOwners;
   private final long timeout;
   private final CacheMode cacheMode;

   // Per-node configuration
   private final float capacityFactor;

   // Per-node state info
   private final PersistentUUID persistentUUID;
   private final Optional<Integer> persistentStateChecksum;

   public CacheJoinInfo(ConsistentHashFactory consistentHashFactory, int numSegments, int numOwners, long timeout,
                        CacheMode cacheMode, float capacityFactor,
                        PersistentUUID persistentUUID, Optional<Integer> persistentStateChecksum) {
      this.consistentHashFactory = consistentHashFactory;
      this.numSegments = numSegments;
      this.numOwners = numOwners;
      this.timeout = timeout;
      this.cacheMode = cacheMode;
      this.capacityFactor = capacityFactor;
      this.persistentUUID = persistentUUID;
      this.persistentStateChecksum = persistentStateChecksum;
   }

   @ProtoFactory
   CacheJoinInfo(MarshallableObject<ConsistentHashFactory<?>> wrappedConsistentHashFactory, int numSegments, int numOwners,
                 long timeout, CacheMode cacheMode, float capacityFactor,
                 PersistentUUID persistentUUID, Integer persistentStateChecksum) {
      this(MarshallableObject.unwrap(wrappedConsistentHashFactory), numSegments, numOwners, timeout, cacheMode, capacityFactor,
            persistentUUID, Optional.ofNullable(persistentStateChecksum));
   }

   public ConsistentHashFactory getConsistentHashFactory() {
      return consistentHashFactory;
   }

   @ProtoField(number = 1)
   MarshallableObject<ConsistentHashFactory<?>> getWrappedConsistentHashFactory() {
      return MarshallableObject.create(consistentHashFactory);
   }

   @ProtoField(number = 2, defaultValue = "-1")
   public int getNumSegments() {
      return numSegments;
   }

   @ProtoField(number = 3, defaultValue = "-1")
   public int getNumOwners() {
      return numOwners;
   }

   @ProtoField(number = 4, defaultValue = "-1")
   public long getTimeout() {
      return timeout;
   }

   @ProtoField(number = 6)
   public CacheMode getCacheMode() {
      return cacheMode;
   }

   @ProtoField(number = 7, defaultValue = "0.0")
   public float getCapacityFactor() {
      return capacityFactor;
   }

   @ProtoField(number = 8)
   public PersistentUUID getPersistentUUID() {
      return persistentUUID;
   }

   @ProtoField(number = 9)
   public Optional<Integer> getPersistentStateChecksum() {
      return persistentStateChecksum;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Float.floatToIntBits(capacityFactor);
      result = prime * result + ((consistentHashFactory == null) ? 0 : consistentHashFactory.hashCode());
      result = prime * result + cacheMode.hashCode();
      result = prime * result + numOwners;
      result = prime * result + numSegments;
      result = prime * result + (int) (timeout ^ (timeout >>> 32));
      result = prime * result + ((persistentUUID == null) ? 0 : persistentUUID.hashCode());
      result = prime * result + ((persistentStateChecksum == null) ? 0 : persistentStateChecksum.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      CacheJoinInfo other = (CacheJoinInfo) obj;
      if (Float.floatToIntBits(capacityFactor) != Float.floatToIntBits(other.capacityFactor))
         return false;
      if (consistentHashFactory == null) {
         if (other.consistentHashFactory != null)
            return false;
      } else if (!consistentHashFactory.equals(other.consistentHashFactory))
         return false;
      if (cacheMode != other.cacheMode)
         return false;
      if (numOwners != other.numOwners)
         return false;
      if (numSegments != other.numSegments)
         return false;
      if (timeout != other.timeout)
         return false;
      if (persistentUUID == null) {
         if (other.persistentUUID != null)
            return false;
      } else if (!persistentUUID.equals(other.persistentUUID))
         return false;
      if (persistentStateChecksum == null) {
         if (other.persistentStateChecksum != null)
            return false;
      } else if (!persistentStateChecksum.equals(other.persistentStateChecksum))
         return false;
      return true;
   }

   @Override
   public String toString() {
      return "CacheJoinInfo{" +
            "consistentHashFactory=" + consistentHashFactory +
            ", numSegments=" + numSegments +
            ", numOwners=" + numOwners +
            ", timeout=" + timeout +
            ", cacheMode=" + cacheMode +
            ", persistentUUID=" + persistentUUID +
            ", persistentStateChecksum=" + persistentStateChecksum +
            ", capacityFactor=" + getCapacityFactor() +
            '}';
   }
}
