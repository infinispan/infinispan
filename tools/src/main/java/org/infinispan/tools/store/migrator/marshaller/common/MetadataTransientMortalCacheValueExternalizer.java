package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataTransientMortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataTransientMortalCacheValueExternalizer extends AbstractMigratorExternalizer<MetadataTransientMortalCacheValue> {

   public MetadataTransientMortalCacheValueExternalizer() {
      super(MetadataTransientMortalCacheValue.class, Ids.METADATA_TRANSIENT_MORTAL_VALUE);
   }

   @Override
   public MetadataTransientMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      return new MetadataTransientMortalCacheValue(value, metadata, created, lastUsed);
   }
}
