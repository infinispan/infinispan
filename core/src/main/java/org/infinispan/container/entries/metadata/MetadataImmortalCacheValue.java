package org.infinispan.container.entries.metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * A form of {@link ImmortalCacheValue} that is {@link MetadataAware}.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataImmortalCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;

   public MetadataImmortalCacheValue(Object value, Metadata metadata) {
      this(value, null, metadata);
   }

   protected MetadataImmortalCacheValue(Object value, PrivateMetadata internalMetadata, Metadata metadata) {
      super(value, internalMetadata);
      this.metadata = metadata;
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

   public static class Externalizer extends AbstractExternalizer<MetadataImmortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataImmortalCacheValue icv) throws IOException {
         output.writeObject(icv.value);
         output.writeObject(icv.internalMetadata);
         output.writeObject(icv.metadata);
      }

      @Override
      public MetadataImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object value = input.readObject();
         PrivateMetadata internalMetadata = (PrivateMetadata) input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         return new MetadataImmortalCacheValue(value, internalMetadata, metadata);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_IMMORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataImmortalCacheValue>> getTypeClasses() {
         return Collections.singleton(MetadataImmortalCacheValue.class);
      }
   }

}
