package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataImmortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataImmortalCacheEntryExternalizer implements AdvancedExternalizer<MetadataImmortalCacheEntry> {

   @Override
   public Set<Class<? extends MetadataImmortalCacheEntry>> getTypeClasses() {
      return Collections.singleton(MetadataImmortalCacheEntry.class);
   }

   @Override
   public Integer getId() {
      return Ids.METADATA_IMMORTAL_ENTRY;
   }

   @Override
   public void writeObject(ObjectOutput output, MetadataImmortalCacheEntry ice) throws IOException {
      output.writeObject(ice.getKey());
      output.writeObject(ice.getValue());
      output.writeObject(ice.getMetadata());
   }

   @Override
   public MetadataImmortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      return new MetadataImmortalCacheEntry(key, value, metadata);
   }

}
