package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataMortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataMortalCacheValueExternalizer implements AdvancedExternalizer<MetadataMortalCacheValue> {

   @Override
   public Set<Class<? extends MetadataMortalCacheValue>> getTypeClasses() {
      return Collections.singleton(MetadataMortalCacheValue.class);
   }

   @Override
   public Integer getId() {
      return Ids.METADATA_MORTAL_VALUE;
   }

   @Override
   public void writeObject(ObjectOutput output, MetadataMortalCacheValue icv) throws IOException {
      output.writeObject(icv.getValue());
      output.writeObject(icv.getMetadata());
      UnsignedNumeric.writeUnsignedLong(output, icv.getCreated());
   }

   @Override
   public MetadataMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      return new MetadataMortalCacheValue(value, metadata, created);
   }

}
