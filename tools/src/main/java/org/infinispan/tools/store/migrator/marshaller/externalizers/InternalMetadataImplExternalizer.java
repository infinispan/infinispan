package org.infinispan.tools.store.migrator.marshaller.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;

/**
 * Externalizer for ${@link InternalMetadataImpl}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class InternalMetadataImplExternalizer implements AdvancedExternalizer<InternalMetadataImpl> {

   private static final long serialVersionUID = -5291318076267612501L;

   @Override
   public void writeObject(ObjectOutput output, InternalMetadataImpl b) throws IOException {
      output.writeLong(b.created());
      output.writeLong(b.lastUsed());
      output.writeObject(b.actual());
   }

   @Override
   public InternalMetadataImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      long created = input.readLong();
      long lastUsed = input.readLong();
      Metadata actual = (Metadata) input.readObject();
      return new InternalMetadataImpl(actual, created, lastUsed);
   }

   @Override
   public Integer getId() {
      return Ids.INTERNAL_METADATA_ID;
   }

   @Override
   @SuppressWarnings("unchecked")
   public Set<Class<? extends InternalMetadataImpl>> getTypeClasses() {
      return Util.asSet(InternalMetadataImpl.class);
   }
}
