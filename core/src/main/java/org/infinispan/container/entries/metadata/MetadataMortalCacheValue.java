package org.infinispan.container.entries.metadata;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A mortal cache value, to correspond with {@link MetadataMortalCacheEntry}
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@ProtoTypeId(ProtoStreamTypeIds.METADATA_MORTAL_VALUE)
public class MetadataMortalCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;
   long created;

   public MetadataMortalCacheValue(Object value, Metadata metadata, long created) {
      this(value, null, metadata, created);
   }

   protected MetadataMortalCacheValue(Object value, PrivateMetadata internalMetadata, Metadata metadata, long created) {
      super(value, internalMetadata);
      this.metadata = metadata;
      this.created = created;
   }

   @ProtoFactory
   MetadataMortalCacheValue(MarshallableObject<?> wrappedValue, PrivateMetadata internalMetadata,
                            MarshallableObject<Metadata> wrappedMetadata, long created) {
      super(wrappedValue, internalMetadata);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.created = created;
   }

   @ProtoField(number = 3, name ="metadata")
   public MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   @ProtoField(4)
   public final long getCreated() {
      return created;
   }

   @Override
   public InternalCacheEntry<?, ?> toInternalCacheEntry(Object key) {
      return new MetadataMortalCacheEntry(key, value, internalMetadata, metadata, created);
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
   public final long getLifespan() {
      return metadata.lifespan();
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(metadata.lifespan(), created, now);
   }

   @Override
   public long getExpiryTime() {
      long lifespan = metadata.lifespan();
      return lifespan > -1 ? created + lifespan : -1;
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
      builder.append(", created=").append(created);
   }
}
