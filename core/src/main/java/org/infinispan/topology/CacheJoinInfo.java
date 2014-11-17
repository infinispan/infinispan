package org.infinispan.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.commons.marshall.AbstractExternalizer;
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
   private final boolean distributed;

   // Per-node configuration
   private final float capacityFactor;

   public CacheJoinInfo(ConsistentHashFactory consistentHashFactory, Hash hashFunction, int numSegments,
                        int numOwners, long timeout, boolean totalOrder, boolean distributed, float capacityFactor) {
      this.consistentHashFactory = consistentHashFactory;
      this.hashFunction = hashFunction;
      this.numSegments = numSegments;
      this.numOwners = numOwners;
      this.timeout = timeout;
      this.totalOrder = totalOrder;
      this.distributed = distributed;
      this.capacityFactor = capacityFactor;
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

   public boolean isDistributed() {
      return distributed;
   }

   public float getCapacityFactor() {
      return capacityFactor;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Float.floatToIntBits(capacityFactor);
      result = prime * result + ((consistentHashFactory == null) ? 0 : consistentHashFactory.hashCode());
      result = prime * result + (distributed ? 1231 : 1237);
      result = prime * result + ((hashFunction == null) ? 0 : hashFunction.hashCode());
      result = prime * result + numOwners;
      result = prime * result + numSegments;
      result = prime * result + (int) (timeout ^ (timeout >>> 32));
      result = prime * result + (totalOrder ? 1231 : 1237);
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
      if (distributed != other.distributed)
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
            ", distributed=" + distributed +
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
         output.writeBoolean(cacheJoinInfo.distributed);
         output.writeFloat(cacheJoinInfo.capacityFactor);
      }

      @Override
      public CacheJoinInfo readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         ConsistentHashFactory consistentHashFactory = (ConsistentHashFactory) unmarshaller.readObject();
         Hash hashFunction = (Hash) unmarshaller.readObject();
         int numSegments = unmarshaller.readInt();
         int numOwners = unmarshaller.readInt();
         long timeout = unmarshaller.readLong();
         boolean totalOrder = unmarshaller.readBoolean();
         boolean distributed = unmarshaller.readBoolean();
         float capacityFactor = unmarshaller.readFloat();
         return new CacheJoinInfo(consistentHashFactory, hashFunction, numSegments, numOwners, timeout,
               totalOrder, distributed, capacityFactor);
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
