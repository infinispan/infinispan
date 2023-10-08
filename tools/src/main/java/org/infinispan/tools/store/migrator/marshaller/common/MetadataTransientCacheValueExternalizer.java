package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataTransientCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataTransientCacheValueExternalizer extends AbstractMigratorExternalizer<MetadataTransientCacheValue> {

   public MetadataTransientCacheValueExternalizer() {
      super(MetadataTransientCacheValue.class, Ids.METADATA_TRANSIENT_VALUE);
   }

   @Override
   public MetadataTransientCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      return new MetadataTransientCacheValue(value, metadata, lastUsed);
   }
}
