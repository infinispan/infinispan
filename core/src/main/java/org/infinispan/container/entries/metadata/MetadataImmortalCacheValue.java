package org.infinispan.container.entries.metadata;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A form of {@link ImmortalCacheValue} that is {@link MetadataAware}.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@ProtoTypeId(ProtoStreamTypeIds.METADATA_IMMORTAL_VALUE)
public class MetadataImmortalCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;

   public MetadataImmortalCacheValue(Object value, Metadata metadata) {
      this(value, null, metadata);
   }

   protected MetadataImmortalCacheValue(Object value, PrivateMetadata internalMetadata, Metadata metadata) {
      super(value, internalMetadata);
      this.metadata = metadata;
   }

   @ProtoFactory
   MetadataImmortalCacheValue(MarshallableObject<?> wrappedValue, PrivateMetadata internalMetadata,
                              MarshallableObject<Metadata> wrappedMetadata) {
      super(wrappedValue, internalMetadata);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
   }

   @ProtoField(number = 3, name = "metadata")
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   public InternalCacheEntry<?, ?> toInternalCacheEntry(Object key) {
      return new MetadataImmortalCacheEntry(key, value, internalMetadata, metadata);
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
   }
}
