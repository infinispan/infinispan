package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.metadata.Metadata;

/**
 * Externalizer for {@link MetadataImmortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MetadataImmortalCacheValueExternalizer extends AbstractMigratorExternalizer<MetadataImmortalCacheValue> {

   public MetadataImmortalCacheValueExternalizer() {
      super(MetadataImmortalCacheValue.class, Ids.METADATA_IMMORTAL_VALUE);
   }

   @Override
   public MetadataImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object v = input.readObject();
      Metadata metadata = (Metadata) input.readObject();
      return new MetadataImmortalCacheValue(v, metadata);
   }
}
