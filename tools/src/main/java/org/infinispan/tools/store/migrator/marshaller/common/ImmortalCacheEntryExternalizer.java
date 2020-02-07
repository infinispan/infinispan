package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer for {@link ImmortalCacheEntry}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class ImmortalCacheEntryExternalizer implements AdvancedExternalizer<ImmortalCacheEntry> {

   @Override
   public Set<Class<? extends ImmortalCacheEntry>> getTypeClasses() {
      return Collections.singleton(ImmortalCacheEntry.class);
   }

   @Override
   public Integer getId() {
      return Ids.IMMORTAL_ENTRY;
   }

   @Override
   public void writeObject(ObjectOutput output, ImmortalCacheEntry ice) throws IOException {
      output.writeObject(ice.getKey());
      output.writeObject(ice.getValue());
   }

   @Override
   public ImmortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      return new ImmortalCacheEntry(key, value);
   }

}
