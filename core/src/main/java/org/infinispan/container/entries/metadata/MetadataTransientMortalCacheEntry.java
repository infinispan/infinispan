package org.infinispan.container.entries.metadata;

import static java.lang.Math.min;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.AbstractInternalCacheEntry;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A form of {@link TransientMortalCacheEntry} that stores {@link Metadata}
 *
 * @author Manik Surtani
 * @since 5.1
 */
@ProtoTypeId(ProtoStreamTypeIds.METADATA_TRANSIENT_MORTAL_CACHE_ENTRY)
public class MetadataTransientMortalCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   Metadata metadata;
   long created;
   long lastUsed;

   public MetadataTransientMortalCacheEntry(Object key, Object value, Metadata metadata, long now) {
      this(key, value, metadata, now, now);
   }

   public MetadataTransientMortalCacheEntry(Object key, Object value, Metadata metadata, long lastUsed, long created) {
      this(key, value, null, metadata, lastUsed, created);
   }

   protected MetadataTransientMortalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata,
         Metadata metadata, long lastUsed, long created) {
      super(key, value, internalMetadata);
      this.metadata = metadata;
      this.lastUsed = lastUsed;
      this.created = created;
   }

   @ProtoFactory
   MetadataTransientMortalCacheEntry(MarshallableObject<?> wrappedKey, MarshallableObject<?> wrappedValue,
                                     PrivateMetadata internalMetadata, MarshallableObject<Metadata> wrappedMetadata,
                                     long created, long lastUsed) {
      super(wrappedKey, wrappedValue, internalMetadata);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.created = created;
      this.lastUsed = lastUsed;
   }

   @ProtoField(number = 4, name ="metadata")
   public MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
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
   public long getLifespan() {
      return metadata.lifespan();
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransientMortal(
            metadata.maxIdle(), lastUsed, metadata.lifespan(), created, now);
   }

   @Override
   public boolean canExpireMaxIdle() {
      return true;
   }

   @Override
   public final long getExpiryTime() {
      long lifespan = metadata.lifespan();
      long lset = lifespan > -1 ? created + lifespan : -1;
      long maxIdle = metadata.maxIdle();
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
      return new MetadataTransientMortalCacheValue(value, internalMetadata, metadata, created, lastUsed);
   }

   @Override
   public final void touch(long currentTimeMillis) {
      lastUsed = currentTimeMillis;
   }

   @Override
   public void reincarnate(long now) {
      created = now;
   }

   @Override
   public long getMaxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
      builder.append(", created=").append(created);
      builder.append(", lastUsed=").append(lastUsed);
   }
}
