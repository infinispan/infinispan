package org.infinispan.container.entries;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A cache entry that is mortal.  I.e., has a lifespan.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MORTAL_CACHE_ENTRY)
public class MortalCacheEntry extends AbstractInternalCacheEntry {

   protected long lifespan;
   protected long created;

   public MortalCacheEntry(Object key, Object value, long lifespan, long created) {
      this(key, value, null, lifespan, created);
   }

   protected MortalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata, long lifespan,
         long created) {
      super(key, value, internalMetadata);
      this.lifespan = lifespan;
      this.created = created;
   }

   @ProtoFactory
   MortalCacheEntry(MarshallableObject<?> wrappedKey, MarshallableObject<?> wrappedValue,
                    PrivateMetadata internalMetadata, long created, long lifespan) {
      super(wrappedKey, wrappedValue, internalMetadata);
      this.created = created;
      this.lifespan = lifespan;
   }

   @Override
   @ProtoField(4)
   public final long getCreated() {
      return created;
   }

   @Override
   @ProtoField(5)
   public final long getLifespan() {
      return lifespan;
   }

   @Override
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(lifespan, created, now);
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   public void setLifespan(long lifespan) {
      this.lifespan = lifespan;
   }

   @Override
   public final long getLastUsed() {
      return -1;
   }

   @Override
   public final long getMaxIdle() {
      return -1;
   }

   @Override
   public final long getExpiryTime() {
      return lifespan > -1 ? created + lifespan : -1;
   }

   @Override
   public final void touch(long currentTimeMillis) {
      // no-op
   }

   @Override
   public void reincarnate(long now) {
      this.created = now;
   }

   @Override
   public InternalCacheValue<?> toInternalCacheValue() {
      return new MortalCacheValue(value, internalMetadata, created, lifespan);
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder().lifespan(lifespan).build();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on mortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public MortalCacheEntry clone() {
      return (MortalCacheEntry) super.clone();
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", created=").append(created);
      builder.append(", lifespan=").append(lifespan);
   }
}
