package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.container.entries.ImmortalCacheValue;

/**
 * Externalizer for {@link ImmortalCacheValue}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class ImmortalCacheValueExternalizer extends AbstractMigratorExternalizer<ImmortalCacheValue> {

   public ImmortalCacheValueExternalizer() {
      super(ImmortalCacheValue.class, Ids.IMMORTAL_VALUE);
   }

   @Override
   public ImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object v = input.readObject();
      return new ImmortalCacheValue(v, null);
   }
}
