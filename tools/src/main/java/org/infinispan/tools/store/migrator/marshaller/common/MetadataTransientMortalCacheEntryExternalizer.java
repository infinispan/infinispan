package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataTransientMortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataTransientMortalCacheEntryExternalizer extends AbstractMigratorExternalizer<MetadataTransientMortalCacheEntry> {

   public MetadataTransientMortalCacheEntryExternalizer() {
      super(MetadataTransientMortalCacheEntry.class, Ids.METADATA_TRANSIENT_MORTAL_ENTRY);
   }

   @Override
   public MetadataTransientMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      return new MetadataTransientMortalCacheEntry(key, value, metadata, lastUsed, created);
   }
}
