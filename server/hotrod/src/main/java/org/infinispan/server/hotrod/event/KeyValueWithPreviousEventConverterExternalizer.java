package org.infinispan.server.hotrod.event;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

/**
 * Externalizer for KeyValueWithPreviousEventConverter
 *
 * @author gustavonalle
 * @since 7.2
 */
public class KeyValueWithPreviousEventConverterExternalizer extends AbstractExternalizer<KeyValueWithPreviousEventConverter> {
   @Override
   public Set<Class<? extends KeyValueWithPreviousEventConverter>> getTypeClasses() {
      return Util.asSet(KeyValueWithPreviousEventConverter.class);
   }

   @Override
   public void writeObject(ObjectOutput output, KeyValueWithPreviousEventConverter object) throws IOException {
   }

   @Override
   public KeyValueWithPreviousEventConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return new KeyValueWithPreviousEventConverter();
   }
}
