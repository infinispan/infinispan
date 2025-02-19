package org.infinispan.container.entries;

import static java.lang.Math.min;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * A cache entry that is both transient and mortal.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientMortalCacheEntry extends AbstractInternalCacheEntry {

   protected long maxIdle;
   protected long lastUsed;
   protected long lifespan;
   protected long created;

   public TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan, long currentTimeMillis) {
      this(key, value, maxIdle, lifespan, currentTimeMillis, currentTimeMillis);
   }

   public TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan, long lastUsed,
         long created) {
      this(key, value, null, maxIdle, lifespan, lastUsed, created);
   }

   protected TransientMortalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata,
         long maxIdle, long lifespan, long lastUsed, long created) {
      super(key, value, internalMetadata);
      this.maxIdle = maxIdle;
      this.lifespan = lifespan;
      this.created = created;
      this.lastUsed = lastUsed;
   }

   @ProtoFactory
   TransientMortalCacheEntry(MarshallableObject<?> wrappedKey, MarshallableObject<?> wrappedValue,
                             PrivateMetadata internalMetadata, long maxIdle, long lastUsed,
                             long lifespan, long created) {
      super(wrappedKey, wrappedValue, internalMetadata);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
      this.lifespan = lifespan;
      this.created = created;
   }

   @Override
   @ProtoField(4)
   public long getLifespan() {
      return lifespan;
   }

   @Override
   @ProtoField(5)
   public long getCreated() {
      return created;
   }

   @Override
   @ProtoField(6)
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   @ProtoField(7)
   public long getMaxIdle() {
      return maxIdle;
   }

   public void setLifespan(long lifespan) {
      this.lifespan = lifespan;
   }

   public void setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
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
      return ExpiryHelper.isExpiredTransientMortal(maxIdle, lastUsed, lifespan, created, now);
   }

   @Override
   public final long getExpiryTime() {
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
   public InternalCacheValue<?> toInternalCacheValue() {
      return new TransientMortalCacheValue(value, internalMetadata, created, lifespan, maxIdle, lastUsed);
   }

   @Override
   public final void touch(long currentTimeMillis) {
      this.lastUsed = currentTimeMillis;
   }

   @Override
   public void reincarnate(long now) {
      this.created = now;
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder()
            .lifespan(lifespan)
            .maxIdle(maxIdle).build();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on mortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public TransientMortalCacheEntry clone() {
      return (TransientMortalCacheEntry) super.clone();
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", lastUsed=").append(lastUsed);
      builder.append(", maxIdle=").append(maxIdle);
      builder.append(", created=").append(created);
      builder.append(", lifespan=").append(lifespan);
   }
}
