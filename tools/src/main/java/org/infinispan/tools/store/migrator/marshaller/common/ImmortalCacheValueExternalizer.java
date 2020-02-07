package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer for {@link ImmortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class ImmortalCacheValueExternalizer implements AdvancedExternalizer<ImmortalCacheValue> {

   @Override
   public Set<Class<? extends ImmortalCacheValue>> getTypeClasses() {
      return Collections.singleton(ImmortalCacheValue.class);
   }

   @Override
   public Integer getId() {
      return Ids.IMMORTAL_VALUE;
   }

   @Override
   public void writeObject(ObjectOutput output, ImmortalCacheValue icv) throws IOException {
      output.writeObject(icv.getValue());
   }

   @Override
   public ImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object value = input.readObject();
      return new ImmortalCacheValue(value);
   }

}
