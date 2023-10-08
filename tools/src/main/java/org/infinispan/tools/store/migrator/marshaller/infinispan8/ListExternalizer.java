package org.infinispan.tools.store.migrator.marshaller.infinispan8;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.tools.store.migrator.marshaller.common.AbstractMigratorExternalizer;

import net.jcip.annotations.Immutable;

/**
 * List externalizer dealing with ArrayList and LinkedList implementations.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
class ListExternalizer extends AbstractMigratorExternalizer<List> {

   private static final int ARRAY_LIST = 0;
   private static final int LINKED_LIST = 1;

   public ListExternalizer() {
      super(Set.of(ArrayList.class, LinkedList.class, getPrivateArrayListClass()), ExternalizerTable.ARRAY_LIST);
   }

   @Override
   public List readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      switch (magicNumber) {
         case ARRAY_LIST:
            return MarshallUtil.unmarshallCollection(input, ArrayList::new);
         case LINKED_LIST:
            return MarshallUtil.unmarshallCollection(input, s -> new LinkedList<>());
         default:
            throw new IllegalStateException("Unknown List type: " + magicNumber);
      }
   }

   private static Class<List> getPrivateArrayListClass() {
      return Util.<List>loadClass("java.util.Arrays$ArrayList", List.class.getClassLoader());
   }
}
