package org.infinispan.marshall.exts;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;

/**
 * Externalizers for diverse array types.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class ArrayExternalizers {

   public static class ListArray implements AdvancedExternalizer<List[]> {

      @Override
      public void writeObject(ObjectOutput output, List[] lists) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, lists.length);
         for (List l : lists)
            output.writeObject(l);
      }

      @Override
      public List[] readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int len = UnsignedNumeric.readUnsignedInt(input);
         List[] lists = new List[len];
         for (int i = 0; i < len; i++)
            lists[i] = (List) input.readObject();

         return lists;
      }

      @Override
      public Integer getId() {
         return Ids.LIST_ARRAY;
      }

      @Override
      @SuppressWarnings("unchecked") // on purpose, it would not work otherwise
      public Set getTypeClasses() {
         return Util.asSet(List[].class);
      }

   }

}
