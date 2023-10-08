package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.TransientMortalCacheValue;

/**
 * Externalizer for {@link TransientMortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class TransientMortalCacheValueExternalizer extends AbstractMigratorExternalizer<TransientMortalCacheValue> {

   public TransientMortalCacheValueExternalizer() {
      super(TransientMortalCacheValue.class, Ids.TRANSIENT_MORTAL_VALUE);
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
