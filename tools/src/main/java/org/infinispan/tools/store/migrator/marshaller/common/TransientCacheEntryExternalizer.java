package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.TransientCacheEntry;

/**
 * Externalizer for {@link TransientCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class TransientCacheEntryExternalizer extends AbstractMigratorExternalizer<TransientCacheEntry> {

   public TransientCacheEntryExternalizer() {
      super(TransientCacheEntry.class, Ids.TRANSIENT_ENTRY);
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
