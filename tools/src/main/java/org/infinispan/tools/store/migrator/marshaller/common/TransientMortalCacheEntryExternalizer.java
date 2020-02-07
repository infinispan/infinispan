package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer for {@link TransientMortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class TransientMortalCacheEntryExternalizer implements AdvancedExternalizer<TransientMortalCacheEntry> {

   @Override
   public Set<Class<? extends TransientMortalCacheEntry>> getTypeClasses() {
      return Collections.singleton(TransientMortalCacheEntry.class);
   }

   @Override
   public Integer getId() {
      return Ids.TRANSIENT_MORTAL_ENTRY;
   }

   @Override
   public void writeObject(ObjectOutput output, TransientMortalCacheEntry ice) throws IOException {
      output.writeObject(ice.getKey());
      output.writeObject(ice.getValue());
      UnsignedNumeric.writeUnsignedLong(output, ice.getCreated());
      output.writeLong(ice.getLifespan());
      UnsignedNumeric.writeUnsignedLong(output, ice.getLastUsed());
      output.writeLong(ice.getMaxIdle());
   }

   @Override
   public TransientMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      long lifespan = input.readLong();
      long lastUsed = UnsignedNumeric.readUnsignedLong(input);
      long maxIdle = input.readLong();
      return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, lastUsed, created);
   }

}
