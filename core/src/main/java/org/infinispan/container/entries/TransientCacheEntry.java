package org.infinispan.container.entries;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A cache entry that is transient, i.e., it can be considered expired after a period of not being used.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.TRANSIENT_CACHE_ENTRY)
public class TransientCacheEntry extends AbstractInternalCacheEntry {

   protected long maxIdle;
   protected long lastUsed;

   public TransientCacheEntry(Object key, Object value, long maxIdle, long lastUsed) {
      this(key, value, null, maxIdle, lastUsed);
   }

   protected TransientCacheEntry(Object key, Object value, PrivateMetadata internalMetadata, long maxIdle,
         long lastUsed) {
      super(key, value, internalMetadata);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   @ProtoFactory
   TransientCacheEntry(MarshallableObject<?> wrappedKey, MarshallableObject<?> wrappedValue,
                       PrivateMetadata internalMetadata, long maxIdle, long lastUsed) {
      super(wrappedKey, wrappedValue, internalMetadata);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   @Override
   @ProtoField(number = 4, defaultValue = "-1")
   public final long getMaxIdle() {
      return maxIdle;
   }

   @Override
   @ProtoField(number = 5, defaultValue = "-1")
   public final long getLastUsed() {
      return lastUsed;
   }

   @Override
   public final void touch(long currentTimeMillis) {
      this.lastUsed = currentTimeMillis;
   }

   @Override
   public void reincarnate(long now) {
      // no-op
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public boolean canExpireMaxIdle() {
      return true;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransient(maxIdle, lastUsed, now);
   }

   public void setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getExpiryTime() {
      return maxIdle > -1 ? lastUsed + maxIdle : -1;
   }

   @Override
   public InternalCacheValue<?> toInternalCacheValue() {
      return new TransientCacheValue(value, internalMetadata, maxIdle, lastUsed);
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder()
            .maxIdle(maxIdle, TimeUnit.MILLISECONDS).build();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on mortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public TransientCacheEntry clone() {
      return (TransientCacheEntry) super.clone();
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", lastUsed=").append(lastUsed);
      builder.append(", maxIdle=").append(maxIdle);
   }
}
