package org.infinispan.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.marshall.core.Ids;

/**
 * This class contains the information that a cache needs to supply to the coordinator when starting up.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class CacheJoinInfo {
   // Global configuration
   private final ConsistentHashFactory consistentHashFactory;
   private final Hash hashFunction;
   private final int numSegments;
   private final int numOwners;
   private final long timeout;
   private final boolean totalOrder;
   private final CacheMode cacheMode;
   private final boolean partitionHandling;

   // Per-node configuration
   private final float capacityFactor;

   // Per-node state info
   private final PersistentUUID persistentUUID;
   private final Optional<Integer> persistentStateChecksum;

   public CacheJoinInfo(ConsistentHashFactory consistentHashFactory, Hash hashFunction, int numSegments,
                        int numOwners, long timeout, boolean totalOrder, CacheMode cacheMode, boolean partitionHandling, float capacityFactor,
                        PersistentUUID persistentUUID,
                        Optional<Integer> persistentStateChecksum) {
      this.consistentHashFactory = consistentHashFactory;
      this.hashFunction = hashFunction;
      this.numSegments = numSegments;
      this.numOwners = numOwners;
      this.timeout = timeout;
      this.totalOrder = totalOrder;
      this.cacheMode = cacheMode;
      this.partitionHandling = partitionHandling;
      this.capacityFactor = capacityFactor;
      this.persistentUUID = persistentUUID;
      this.persistentStateChecksum = persistentStateChecksum;
   }

   public ConsistentHashFactory getConsistentHashFactory() {
      return consistentHashFactory;
   }

   public Hash getHashFunction() {
      return hashFunction;
   }

   public int getNumSegments() {
      return numSegments;
   }

   public int getNumOwners() {
      return numOwners;
   }

   public long getTimeout() {
      return timeout;
   }

   public boolean isTotalOrder() {
      return totalOrder;
   }

   public CacheMode getCacheMode() {
      return cacheMode;
   }

   public boolean isPartitionHandling() {
      return partitionHandling;
   }

   public float getCapacityFactor() {
      return capacityFactor;
   }

   public PersistentUUID getPersistentUUID() {
      return persistentUUID;
   }

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
      result = prime * result + (partitionHandling ? 1231 : 1237);
      result = prime * result + ((hashFunction == null) ? 0 : hashFunction.hashCode());
      result = prime * result + numOwners;
      result = prime * result + numSegments;
      result = prime * result + (int) (timeout ^ (timeout >>> 32));
      result = prime * result + (totalOrder ? 1231 : 1237);
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
      if (partitionHandling != other.partitionHandling)
         return false;
      if (hashFunction == null) {
         if (other.hashFunction != null)
            return false;
      } else if (!hashFunction.equals(other.hashFunction))
         return false;
      if (numOwners != other.numOwners)
         return false;
      if (numSegments != other.numSegments)
         return false;
      if (timeout != other.timeout)
         return false;
      if (totalOrder != other.totalOrder)
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
            ", hashFunction=" + hashFunction +
            ", numSegments=" + numSegments +
            ", numOwners=" + numOwners +
            ", timeout=" + timeout +
            ", totalOrder=" + totalOrder +
            ", cacheMode=" + cacheMode +
            ", partitionHandling=" + partitionHandling +
            ", persistentUUID=" + persistentUUID +
            ", persistentStateChecksum=" + persistentStateChecksum +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<CacheJoinInfo> {
      @Override
      public void writeObject(ObjectOutput output, CacheJoinInfo cacheJoinInfo) throws IOException {
         output.writeObject(cacheJoinInfo.consistentHashFactory);
         output.writeObject(cacheJoinInfo.hashFunction);
         output.writeInt(cacheJoinInfo.numSegments);
         output.writeInt(cacheJoinInfo.numOwners);
         output.writeLong(cacheJoinInfo.timeout);
         output.writeBoolean(cacheJoinInfo.totalOrder);
         MarshallUtil.marshallEnum(cacheJoinInfo.cacheMode, output);
         output.writeBoolean(cacheJoinInfo.partitionHandling);
         output.writeFloat(cacheJoinInfo.capacityFactor);
         output.writeObject(cacheJoinInfo.persistentUUID);
         output.writeObject(cacheJoinInfo.persistentStateChecksum);
      }

      @Override
      public CacheJoinInfo readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         ConsistentHashFactory consistentHashFactory = (ConsistentHashFactory) unmarshaller.readObject();
         Hash hashFunction = (Hash) unmarshaller.readObject();
         int numSegments = unmarshaller.readInt();
         int numOwners = unmarshaller.readInt();
         long timeout = unmarshaller.readLong();
         boolean totalOrder = unmarshaller.readBoolean();
         CacheMode cacheMode = MarshallUtil.unmarshallEnum(unmarshaller, CacheMode::valueOf);
         boolean partitionHandling = unmarshaller.readBoolean();
         float capacityFactor = unmarshaller.readFloat();
         PersistentUUID persistentUUID = (PersistentUUID) unmarshaller.readObject();
         Optional<Integer> persistentStateChecksum = (Optional<Integer>) unmarshaller.readObject();
         return new CacheJoinInfo(consistentHashFactory, hashFunction, numSegments, numOwners, timeout,
               totalOrder, cacheMode, partitionHandling, capacityFactor, persistentUUID, persistentStateChecksum);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_JOIN_INFO;
      }

      @Override
      public Set<Class<? extends CacheJoinInfo>> getTypeClasses() {
         return Collections.<Class<? extends CacheJoinInfo>>singleton(CacheJoinInfo.class);
      }
   }
}
