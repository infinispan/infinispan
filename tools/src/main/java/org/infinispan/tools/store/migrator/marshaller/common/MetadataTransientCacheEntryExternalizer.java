package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataTransientCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataTransientCacheEntryExternalizer extends AbstractMigratorExternalizer<MetadataTransientCacheEntry> {

   public MetadataTransientCacheEntryExternalizer() {
      super(MetadataTransientCacheEntry.class, Ids.METADATA_TRANSIENT_ENTRY);
   }

   @Override
   public MetadataTransientCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      return new MetadataTransientCacheEntry(key, value, metadata, lastUsed);
   }
}
