package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.TransientCacheValue;

/**
 * Externalizer for {@link TransientCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class TransientCacheValueExternalizer extends AbstractMigratorExternalizer<TransientCacheValue> {

   public TransientCacheValueExternalizer() {
      super(TransientCacheValue.class, Ids.TRANSIENT_VALUE);
   }

   @Override
   public TransientCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      long maxIdle = input.readLong();
      return new TransientCacheValue(value, maxIdle, lastUsed);
   }
}
