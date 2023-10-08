package org.infinispan.container.entries.metadata;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.AbstractInternalCacheEntry;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A cache entry that is mortal and is {@link MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@ProtoTypeId(ProtoStreamTypeIds.METADATA_MORTAL_ENTRY)
public class MetadataMortalCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   protected Metadata metadata;
   protected long created;

   public MetadataMortalCacheEntry(Object key, Object value, Metadata metadata, long created) {
      this(key, value, null, metadata, created);
   }

   protected MetadataMortalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata,
         Metadata metadata, long created) {
      super(key, value, internalMetadata);
      this.metadata = metadata;
      this.created = created;
   }

   @ProtoFactory
   MetadataMortalCacheEntry(MarshallableObject<?> wrappedKey, MarshallableObject<?> wrappedValue,
                            PrivateMetadata internalMetadata, MarshallableObject<Metadata> wrappedMetadata,
                            long created) {
      super(wrappedKey, wrappedValue, internalMetadata);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.created = created;
   }

   @ProtoField(number = 4, name ="metadata")
   public MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   @ProtoField(5)
   public final long getCreated() {
      return created;
   }

   @Override
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(metadata.lifespan(), created, now);
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public final long getLastUsed() {
      return -1;
   }

   @Override
   public final long getLifespan() {
      return metadata.lifespan();
   }

   @Override
   public final long getMaxIdle() {
      return -1;
   }

   @Override
   public final long getExpiryTime() {
      long lifespan = metadata.lifespan();
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
      return new MetadataMortalCacheValue(value, internalMetadata, metadata, created);
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
   }
}
