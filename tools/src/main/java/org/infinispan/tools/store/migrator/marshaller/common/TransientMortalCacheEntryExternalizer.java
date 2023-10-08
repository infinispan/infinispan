package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.TransientMortalCacheEntry;

/**
 * Externalizer for {@link TransientMortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class TransientMortalCacheEntryExternalizer extends AbstractMigratorExternalizer<TransientMortalCacheEntry> {

   public TransientMortalCacheEntryExternalizer() {
      super(TransientMortalCacheEntry.class, Ids.TRANSIENT_MORTAL_ENTRY);
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
