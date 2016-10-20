package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.jboss.marshalling.util.IdentityIntMap;

/**
 * Externalizers for diverse array types.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class ArrayExternalizer extends AbstractExternalizer<Object> {

   private static final int OBJECT_ARRAY = 0x00;
   private static final int LIST_ARRAY = 0x01;
   private static final int MAP_ENTRY_ARRAY = 0x02;

   private final IdentityIntMap<Class<?>> subIds = new IdentityIntMap<>(3);

   public ArrayExternalizer() {
      subIds.put(Object[].class, OBJECT_ARRAY);
      subIds.put(List[].class, LIST_ARRAY);
      subIds.put(Map.Entry[].class, MAP_ENTRY_ARRAY);
   }

   @Override
   public Set<Class<?>> getTypeClasses() {
      return Util.asSet(Object[].class, List[].class, Map.Entry[].class);
   }

   @Override
   public Integer getId() {
      return Ids.ARRAYS;
   }

   @Override
   public void writeObject(ObjectOutput out, Object obj) throws IOException {
      int subId = subIds.get(obj.getClass(), -1);
      out.writeByte(subId);
      switch (subId) {
         case OBJECT_ARRAY:
            MarshallUtil.marshallArray((Object[]) obj, out);
            break;
         case LIST_ARRAY:
            MarshallUtil.marshallArray((List[]) obj, out);
            break;
         case MAP_ENTRY_ARRAY:
            MarshallUtil.marshallArray((Map.Entry[]) obj, out);
            break;
      }
   }

   @Override
   public Object readObject(ObjectInput in) throws IOException, ClassNotFoundException {
      int subId = in.readUnsignedByte();
      switch (subId) {
         case OBJECT_ARRAY:
            return MarshallUtil.unmarshallArray(in, Object[]::new);
         case LIST_ARRAY:
            return MarshallUtil.unmarshallArray(in, List[]::new);
         case MAP_ENTRY_ARRAY:
            return MarshallUtil.unmarshallArray(in, Map.Entry[]::new);
         default:
            throw new IllegalStateException("Unknown array type: " + subId);
      }
   }

   // Used only with JBoss Marshalling based Externalizer
   @Deprecated
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
