package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataImmortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataImmortalCacheValueExternalizer implements AdvancedExternalizer<MetadataImmortalCacheValue> {

   @Override
   public Set<Class<? extends MetadataImmortalCacheValue>> getTypeClasses() {
      return Collections.singleton(MetadataImmortalCacheValue.class);
   }

   @Override
   public Integer getId() {
      return Ids.METADATA_IMMORTAL_VALUE;
   }

   @Override
   public void writeObject(ObjectOutput output, MetadataImmortalCacheValue icv) throws IOException {
      output.writeObject(icv.getValue());
      output.writeObject(icv.getMetadata());
   }

   @Override
   public MetadataImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      return new MetadataImmortalCacheValue(value, metadata);
   }

}
