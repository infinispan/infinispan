package org.infinispan.container.entries;

import static java.lang.Math.min;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A transient, mortal cache value to correspond with {@link TransientMortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.TRANSIENT_MORTAL_CACHE_VALUE)
public class TransientMortalCacheValue extends MortalCacheValue {
   protected long maxIdle;
   protected long lastUsed;

   public TransientMortalCacheValue(Object value, long created, long lifespan, long maxIdle, long lastUsed) {
      this(value, null, created, lifespan, maxIdle, lastUsed);
   }

   protected TransientMortalCacheValue(Object value, PrivateMetadata internalMetadata, long created,
         long lifespan, long maxIdle, long lastUsed) {
      super(value, internalMetadata, created, lifespan);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   @ProtoFactory
   TransientMortalCacheValue(MarshallableObject<?> wrappedValue, PrivateMetadata internalMetadata,
                             long created, long lifespan, long maxIdle, long lastUsed) {
      super(wrappedValue, internalMetadata, created, lifespan);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   @Override
   @ProtoField(number = 5, defaultValue = "-1")
   public long getMaxIdle() {
      return maxIdle;
   }

   public void setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
   }

   @Override
   @ProtoField(number = 6, defaultValue = "-1")
   public long getLastUsed() {
      return lastUsed;
   }

   public void setLastUsed(long lastUsed) {
      this.lastUsed = lastUsed;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransientMortal(maxIdle, lastUsed, lifespan, created, now);
   }

   @Override
   public boolean isMaxIdleExpirable() {
      return true;
   }

   @Override
   public InternalCacheEntry<?, ?> toInternalCacheEntry(Object key) {
      return new TransientMortalCacheEntry(key, value, internalMetadata, maxIdle, lifespan, lastUsed, created);
   }

   @Override
   public long getExpiryTime() {
      long lset = lifespan > -1 ? created + lifespan : -1;
      long muet = maxIdle > -1 ? lastUsed + maxIdle : -1;
      if (lset == -1) {
         return muet;
      }
      if (muet == -1) {
         return lset;
      }
      return min(lset, muet);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TransientMortalCacheValue)) return false;
      if (!super.equals(o)) return false;

      TransientMortalCacheValue that = (TransientMortalCacheValue) o;

      return lastUsed == that.lastUsed && maxIdle == that.maxIdle;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
      result = 31 * result + (int) (lastUsed ^ (lastUsed >>> 32));
      return result;
   }

   @Override
   public TransientMortalCacheValue clone() {
      return (TransientMortalCacheValue) super.clone();
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", maxIdle=").append(maxIdle);
      builder.append(", lastUsed=").append(lastUsed);
   }
}
