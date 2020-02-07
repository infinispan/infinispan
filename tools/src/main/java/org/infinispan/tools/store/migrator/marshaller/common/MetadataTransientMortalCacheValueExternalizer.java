package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataTransientMortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataTransientMortalCacheValueExternalizer implements AdvancedExternalizer<MetadataTransientMortalCacheValue> {

   @Override
   public Set<Class<? extends MetadataTransientMortalCacheValue>> getTypeClasses() {
      return Collections.singleton(MetadataTransientMortalCacheValue.class);
   }

   @Override
   public Integer getId() {
      return Ids.METADATA_TRANSIENT_MORTAL_VALUE;
   }

   @Override
   public void writeObject(ObjectOutput output, MetadataTransientMortalCacheValue icv) throws IOException {
      output.writeObject(icv.getValue());
      output.writeObject(icv.getMetadata());
      UnsignedNumeric.writeUnsignedLong(output, icv.getCreated());
      UnsignedNumeric.writeUnsignedLong(output, icv.getLastUsed());
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
