package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer for {@link TransientCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class TransientCacheEntryExternalizer implements AdvancedExternalizer<TransientCacheEntry> {

   @Override
   public Set<Class<? extends TransientCacheEntry>> getTypeClasses() {
      return Collections.singleton(TransientCacheEntry.class);
   }

   @Override
   public Integer getId() {
      return Ids.TRANSIENT_ENTRY;
   }

   @Override
   public void writeObject(ObjectOutput output, TransientCacheEntry ice) throws IOException {
      output.writeObject(ice.getKey());
      output.writeObject(ice.getValue());
      UnsignedNumeric.writeUnsignedLong(output, ice.getLastUsed());
      output.writeLong(ice.getMaxIdle());
   }

   @Override
   public TransientCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      long maxIdle = input.readLong();
      return new TransientCacheEntry(key, value, maxIdle, lastUsed);
   }

}
