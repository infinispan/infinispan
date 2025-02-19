package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.util.KeyValuePair;

public class KeyValuePairExternalizer extends AbstractMigratorExternalizer<KeyValuePair> {

   public KeyValuePairExternalizer() {
      super(KeyValuePair.class, Ids.KEY_VALUE_PAIR_ID);
   }

   @Override
   public KeyValuePair readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return new KeyValuePair(input.readObject(), input.readObject());
   }
}
