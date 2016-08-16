package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

public class OptionalExternalizer extends AbstractExternalizer<Optional> {

   @Override
   public void writeObject(ObjectOutput output, Optional object) throws IOException {
      int isPresent = (object.isPresent() ? 1 : 0);
      output.writeByte(isPresent);
      if (object.isPresent()) output.writeObject(object.get());
   }

   @Override
   public Optional readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      boolean isPresent = input.readByte() == 1;
      return isPresent ? Optional.of(input.readObject()) : Optional.empty();
   }

   @Override
   public Set<Class<? extends Optional>> getTypeClasses() {
      return Util.<Class<? extends Optional>>asSet(Optional.class);
   }

   @Override
   public Integer getId() {
      return Ids.OPTIONAL;
   }

}
