package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataMortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataMortalCacheValueExternalizer extends AbstractMigratorExternalizer<MetadataMortalCacheValue> {

   public MetadataMortalCacheValueExternalizer() {
      super(MetadataMortalCacheValue.class, Ids.METADATA_MORTAL_VALUE);
   }

   @Override
   public MetadataMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      return new MetadataMortalCacheValue(value, metadata, created);
   }
}
