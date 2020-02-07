package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataMortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataMortalCacheEntryExternalizer implements AdvancedExternalizer<MetadataMortalCacheEntry> {

   @Override
   public Set<Class<? extends MetadataMortalCacheEntry>> getTypeClasses() {
      return Collections.singleton(MetadataMortalCacheEntry.class);
   }

   @Override
   public Integer getId() {
      return Ids.METADATA_MORTAL_ENTRY;
   }

   @Override
   public void writeObject(ObjectOutput output, MetadataMortalCacheEntry ice) throws IOException {
      output.writeObject(ice.getKey());
      output.writeObject(ice.getValue());
      output.writeObject(ice.getMetadata());
      UnsignedNumeric.writeUnsignedLong(output, ice.getCreated());
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
