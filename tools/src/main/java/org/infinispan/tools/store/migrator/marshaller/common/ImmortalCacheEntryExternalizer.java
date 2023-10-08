package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.container.entries.ImmortalCacheEntry;

/**
 * Externalizer for {@link ImmortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class ImmortalCacheEntryExternalizer extends AbstractMigratorExternalizer<ImmortalCacheEntry> {

   public ImmortalCacheEntryExternalizer() {
      super(ImmortalCacheEntry.class, Ids.IMMORTAL_ENTRY);
   }

   @Override
   public ImmortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object k = input.readObject();
      Object v = input.readObject();
      return new ImmortalCacheEntry(k, v);
   }
}
