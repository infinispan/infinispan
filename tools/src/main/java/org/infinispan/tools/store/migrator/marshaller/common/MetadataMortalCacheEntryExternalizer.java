package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataMortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataMortalCacheEntryExternalizer extends AbstractMigratorExternalizer<MetadataMortalCacheEntry> {

   public MetadataMortalCacheEntryExternalizer() {
      super(MetadataMortalCacheEntry.class, Ids.METADATA_MORTAL_ENTRY);
   }

   @Override
   public MetadataMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      return new MetadataMortalCacheEntry(key, value, metadata, created);
   }
}
