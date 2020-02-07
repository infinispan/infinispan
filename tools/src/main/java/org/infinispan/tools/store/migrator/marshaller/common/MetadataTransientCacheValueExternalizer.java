package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataTransientCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataTransientCacheValueExternalizer implements AdvancedExternalizer<MetadataTransientCacheValue> {

   @Override
   public Set<Class<? extends MetadataTransientCacheValue>> getTypeClasses() {
      return Collections.singleton(MetadataTransientCacheValue.class);
   }

   @Override
   public Integer getId() {
      return Ids.METADATA_TRANSIENT_VALUE;
   }

   @Override
   public void writeObject(ObjectOutput output, MetadataTransientCacheValue icv) throws IOException {
      output.writeObject(icv.getValue());
      output.writeObject(icv.getMetadata());
      UnsignedNumeric.writeUnsignedLong(output, icv.getLastUsed());
   }

   @Override
   public MetadataTransientCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      return new MetadataTransientCacheValue(value, metadata, lastUsed);
   }

}
