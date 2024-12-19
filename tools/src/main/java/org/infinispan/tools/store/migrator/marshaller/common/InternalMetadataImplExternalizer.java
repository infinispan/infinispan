package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;

/**
 * Externalizer for ${@link InternalMetadataImpl}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class InternalMetadataImplExternalizer extends AbstractMigratorExternalizer<InternalMetadata> {

   public InternalMetadataImplExternalizer(int id) {
      super(InternalMetadataImpl.class, id);
   }

   @Override
   public InternalMetadataImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      long created = input.readLong();
      long lastUsed = input.readLong();
      Metadata actual = (Metadata) input.readObject();
      return new InternalMetadataImpl(actual, created, lastUsed);
   }
}
