package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer for {@link MortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MortalCacheValueExternalizer implements AdvancedExternalizer<MortalCacheValue> {

   @Override
   public Set<Class<? extends MortalCacheValue>> getTypeClasses() {
      return Collections.singleton(MortalCacheValue.class);
   }

   @Override
   public Integer getId() {
      return Ids.MORTAL_VALUE;
   }

   @Override
   public void writeObject(ObjectOutput output, MortalCacheValue icv) throws IOException {
      output.writeObject(icv.getValue());
      UnsignedNumeric.writeUnsignedLong(output, icv.getCreated());
      output.writeLong(icv.getLifespan());
   }

   @Override
   public MortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      long lifespan = input.readLong();
      return new MortalCacheValue(value, created, lifespan);
   }

}
