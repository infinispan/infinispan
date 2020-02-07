package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataTransientMortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataTransientMortalCacheEntryExternalizer implements AdvancedExternalizer<MetadataTransientMortalCacheEntry> {

   @Override
   public Set<Class<? extends MetadataTransientMortalCacheEntry>> getTypeClasses() {
      return Collections.singleton(MetadataTransientMortalCacheEntry.class);
   }

   @Override
   public Integer getId() {
      return Ids.METADATA_TRANSIENT_MORTAL_ENTRY;
   }

   @Override
   public void writeObject(ObjectOutput output, MetadataTransientMortalCacheEntry ice) throws IOException {
      output.writeObject(ice.getKey());
      output.writeObject(ice.getValue());
      output.writeObject(ice.getMetadata());
      UnsignedNumeric.writeUnsignedLong(output, ice.getCreated());
      UnsignedNumeric.writeUnsignedLong(output, ice.getLastUsed());
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
