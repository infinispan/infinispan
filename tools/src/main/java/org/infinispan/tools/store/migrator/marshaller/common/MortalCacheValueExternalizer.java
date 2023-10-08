package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.MortalCacheValue;

/**
 * Externalizer for {@link MortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MortalCacheValueExternalizer extends AbstractMigratorExternalizer<MortalCacheValue> {

   public MortalCacheValueExternalizer() {
      super(MortalCacheValue.class, Ids.MORTAL_VALUE);
   }

   @Override
   public MortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      long lifespan = input.readLong();
      return new MortalCacheValue(value, created, lifespan);
   }
}
