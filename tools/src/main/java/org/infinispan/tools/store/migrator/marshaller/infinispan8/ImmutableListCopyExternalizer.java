package org.infinispan.tools.store.migrator.marshaller.infinispan8;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.util.ImmutableListCopy;
import org.infinispan.tools.store.migrator.marshaller.common.AbstractMigratorExternalizer;

class ImmutableListCopyExternalizer extends AbstractMigratorExternalizer<List> {

   ImmutableListCopyExternalizer() {
      super(Set.of(ImmutableListCopy.class, ImmutableListCopy.ImmutableSubList.class), ExternalizerTable.IMMUTABLE_LIST);
   }

   @Override
   public List readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int size = UnsignedNumeric.readUnsignedInt(input);
      Object[] elements = new Object[size];
      for (int i = 0; i < size; i++)
         elements[i] = input.readObject();

      return new ImmutableListCopy(elements);
   }
}
