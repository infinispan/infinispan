package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.MortalCacheEntry;

/**
 * Externalizer for {@link MortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class MortalCacheEntryExternalizer extends AbstractMigratorExternalizer<MortalCacheEntry> {

   public MortalCacheEntryExternalizer() {
      super(MortalCacheEntry.class, Ids.MORTAL_ENTRY);
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
