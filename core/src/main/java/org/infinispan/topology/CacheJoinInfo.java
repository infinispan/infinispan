package org.infinispan.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

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

   public ConsistentHashFactory getConsistentHashFactory() {
      return consistentHashFactory;
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

   public CacheMode getCacheMode() {
      return cacheMode;
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
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<CacheJoinInfo> {
      @Override
      public void writeObject(ObjectOutput output, CacheJoinInfo cacheJoinInfo) throws IOException {
         output.writeObject(cacheJoinInfo.consistentHashFactory);
         output.writeInt(cacheJoinInfo.numSegments);
         output.writeInt(cacheJoinInfo.numOwners);
         output.writeLong(cacheJoinInfo.timeout);
         MarshallUtil.marshallEnum(cacheJoinInfo.cacheMode, output);
         output.writeFloat(cacheJoinInfo.capacityFactor);
         output.writeObject(cacheJoinInfo.persistentUUID);
         output.writeObject(cacheJoinInfo.persistentStateChecksum);
      }

      @Override
      public CacheJoinInfo readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         ConsistentHashFactory consistentHashFactory = (ConsistentHashFactory) unmarshaller.readObject();
         int numSegments = unmarshaller.readInt();
         int numOwners = unmarshaller.readInt();
         long timeout = unmarshaller.readLong();
         CacheMode cacheMode = MarshallUtil.unmarshallEnum(unmarshaller, CacheMode::valueOf);
         float capacityFactor = unmarshaller.readFloat();
         PersistentUUID persistentUUID = (PersistentUUID) unmarshaller.readObject();
         Optional<Integer> persistentStateChecksum = (Optional<Integer>) unmarshaller.readObject();
         return new CacheJoinInfo(consistentHashFactory, numSegments, numOwners, timeout, cacheMode,
               capacityFactor, persistentUUID, persistentStateChecksum);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_JOIN_INFO;
      }

      @Override
      public Set<Class<? extends CacheJoinInfo>> getTypeClasses() {
         return Collections.singleton(CacheJoinInfo.class);
      }
   }
}
