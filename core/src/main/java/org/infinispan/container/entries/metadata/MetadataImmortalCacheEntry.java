package org.infinispan.container.entries.metadata;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A form of {@link org.infinispan.container.entries.ImmortalCacheEntry} that is {@link
 * org.infinispan.container.entries.metadata.MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@ProtoTypeId(ProtoStreamTypeIds.METADATA_IMMORTAL_ENTRY)
public class MetadataImmortalCacheEntry extends ImmortalCacheEntry implements MetadataAware {

   protected Metadata metadata;

   public MetadataImmortalCacheEntry(Object key, Object value, Metadata metadata) {
      this(key, value, null, metadata);
   }

   protected MetadataImmortalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata, Metadata metadata) {
      super(key, value, internalMetadata);
      this.metadata = metadata;
   }

   @ProtoFactory
   MetadataImmortalCacheEntry(MarshallableObject<?> wrappedKey, MarshallableObject<?> wrappedValue,
                              PrivateMetadata internalMetadata, MarshallableObject<Metadata> wrappedMetadata) {
      super(MarshallableObject.unwrap(wrappedKey), MarshallableObject.unwrap(wrappedValue), internalMetadata);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
   }

   @ProtoField(number = 4)
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
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
   public InternalCacheValue<?> toInternalCacheValue() {
      return new MetadataImmortalCacheValue(value, internalMetadata, metadata);
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
   }
}
