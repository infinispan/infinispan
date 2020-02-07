package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer for {@link MortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MortalCacheEntryExternalizer implements AdvancedExternalizer<MortalCacheEntry> {

   @Override
   public Set<Class<? extends MortalCacheEntry>> getTypeClasses() {
      return Collections.singleton(MortalCacheEntry.class);
   }

   @Override
   public Integer getId() {
      return Ids.MORTAL_ENTRY;
   }

   @Override
   public void writeObject(ObjectOutput output, MortalCacheEntry ice) throws IOException {
      output.writeObject(ice.getKey());
      output.writeObject(ice.getValue());
      UnsignedNumeric.writeUnsignedLong(output, ice.getCreated());
      output.writeLong(ice.getLifespan());
   }

   @Override
   public MortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      long created = UnsignedNumeric.readUnsignedLong(input);
      long lifespan = input.readLong();
      return new MortalCacheEntry(key, value, lifespan, created);
   }

}
