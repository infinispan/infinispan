package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataImmortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataImmortalCacheEntryExternalizer extends AbstractMigratorExternalizer<MetadataImmortalCacheEntry> {

   public MetadataImmortalCacheEntryExternalizer() {
      super(MetadataImmortalCacheEntry.class, Ids.METADATA_IMMORTAL_ENTRY);
   }

   @Override
   public MetadataImmortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      return new MetadataImmortalCacheEntry(key, value, metadata);
   }
}
