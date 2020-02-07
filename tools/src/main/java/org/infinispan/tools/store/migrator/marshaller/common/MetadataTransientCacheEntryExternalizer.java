package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataTransientCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataTransientCacheEntryExternalizer implements AdvancedExternalizer<MetadataTransientCacheEntry> {

   @Override
   public Set<Class<? extends MetadataTransientCacheEntry>> getTypeClasses() {
      return Collections.singleton(MetadataTransientCacheEntry.class);
   }

   @Override
   public Integer getId() {
      return Ids.METADATA_TRANSIENT_ENTRY;
   }

   @Override
   public void writeObject(ObjectOutput output, MetadataTransientCacheEntry ice) throws IOException {
      output.writeObject(ice.getKey());
      output.writeObject(ice.getValue());
      output.writeObject(ice.getMetadata());
      UnsignedNumeric.writeUnsignedLong(output, ice.getLastUsed());
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
