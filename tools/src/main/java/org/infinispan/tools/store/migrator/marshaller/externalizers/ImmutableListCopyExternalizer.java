package org.infinispan.tools.store.migrator.marshaller.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.ImmutableListCopy;
import org.infinispan.commons.util.Util;

public class ImmutableListCopyExternalizer extends AbstractExternalizer<List> {

   @Override
   public void writeObject(ObjectOutput output, List list) throws IOException {
      int size = list.size();
      UnsignedNumeric.writeUnsignedInt(output, size);
      for (int i = 0; i < size; i++) {
         output.writeObject(list.get(i));
      }
   }

   @Override
   public List readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int size = UnsignedNumeric.readUnsignedInt(input);
      Object[] elements = new Object[size];
      for (int i = 0; i < size; i++)
         elements[i] = input.readObject();

      return new ImmutableListCopy(elements);
   }

   @Override
   public Integer getId() {
      return LegacyIds.IMMUTABLE_LIST;
   }

   @Override
   public Set<Class<? extends List>> getTypeClasses() {
      return Util.asSet(ImmutableListCopy.class, ImmutableListCopy.ImmutableSubList.class);
   }
}
