package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer for {@link TransientMortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class TransientMortalCacheValueExternalizer implements AdvancedExternalizer<TransientMortalCacheValue> {

   @Override
   public Set<Class<? extends TransientMortalCacheValue>> getTypeClasses() {
      return Collections.singleton(TransientMortalCacheValue.class);
   }

   @Override
   public Integer getId() {
      return Ids.TRANSIENT_MORTAL_VALUE;
   }

   @Override
   public void writeObject(ObjectOutput output, TransientMortalCacheValue icv) throws IOException {
      output.writeObject(icv.getValue());
      UnsignedNumeric.writeUnsignedLong(output, icv.getCreated());
      output.writeLong(icv.getLifespan());
      UnsignedNumeric.writeUnsignedLong(output, icv.getLastUsed());
      output.writeLong(icv.getMaxIdle());
   }

   @Override
   public TransientMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      long lifespan = input.readLong();
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      long maxIdle = input.readLong();
      return new TransientMortalCacheValue(value, created, lifespan, maxIdle, lastUsed);
   }

}
