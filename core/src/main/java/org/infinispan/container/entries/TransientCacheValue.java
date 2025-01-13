package org.infinispan.container.entries;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A transient cache value, to correspond with {@link TransientCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.TRANSIENT_CACHE_VALUE)
public class TransientCacheValue extends ImmortalCacheValue {
   protected long maxIdle;
   protected long lastUsed;

   public TransientCacheValue(Object value, long maxIdle, long lastUsed) {
      this(value, null, maxIdle, lastUsed);
   }

   protected TransientCacheValue(Object value, PrivateMetadata internalMetadata, long maxIdle, long lastUsed) {
      super(value, internalMetadata);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   @ProtoFactory
   TransientCacheValue(MarshallableObject<?> wrappedValue, PrivateMetadata internalMetadata,
                       long maxIdle, long lastUsed) {
      super(wrappedValue, internalMetadata);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   @Override
   @ProtoField(number = 3, defaultValue = "-1")
   public long getMaxIdle() {
      return maxIdle;
   }

   public void setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
   }

   @Override
   @ProtoField(number = 4, defaultValue = "-1")
   public long getLastUsed() {
      return lastUsed;
   }

   public void setLastUsed(long lastUsed) {
      this.lastUsed = lastUsed;
   }

   @Override
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransient(maxIdle, lastUsed, now);
   }

   @Override
   public boolean canExpire() {
      return true;
   }

   @Override
   public boolean isMaxIdleExpirable() {
      return true;
   }

   @Override
   public InternalCacheEntry<?, ?> toInternalCacheEntry(Object key) {
      return new TransientCacheEntry(key, value, internalMetadata, maxIdle, lastUsed);
   }

   @Override
   public long getExpiryTime() {
      return maxIdle > -1 ? lastUsed + maxIdle : -1;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof TransientCacheValue)) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }

      TransientCacheValue that = (TransientCacheValue) o;

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
   public TransientCacheValue clone() {
      return (TransientCacheValue) super.clone();
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", maxIdle=").append(maxIdle);
      builder.append(", lastUsed=").append(lastUsed);
   }
}
