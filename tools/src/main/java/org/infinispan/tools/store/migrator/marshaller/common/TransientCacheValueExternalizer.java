package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer for {@link TransientCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class TransientCacheValueExternalizer implements AdvancedExternalizer<TransientCacheValue> {

   @Override
   public Set<Class<? extends TransientCacheValue>> getTypeClasses() {
      return Collections.singleton(TransientCacheValue.class);
   }

   @Override
   public Integer getId() {
      return Ids.TRANSIENT_VALUE;
   }

   @Override
   public void writeObject(ObjectOutput output, TransientCacheValue icv) throws IOException {
      output.writeObject(icv.getValue());
      UnsignedNumeric.writeUnsignedLong(output, icv.getLastUsed());
      output.writeLong(icv.getMaxIdle());
   }

   @Override
   public TransientCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      long maxIdle = input.readLong();
      return new TransientCacheValue(value, maxIdle, lastUsed);
   }

}
