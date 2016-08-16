package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.jboss.marshalling.util.IdentityIntMap;

import net.jcip.annotations.Immutable;

/**
 * List externalizer dealing with ArrayList and LinkedList implementations.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class ListExternalizer extends AbstractExternalizer<List> {

   private static final int ARRAY_LIST = 0;
   private static final int LINKED_LIST = 1;
   private static final int EMPTY_LIST = 2;

   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(6);

   public ListExternalizer() {
      numbers.put(ArrayList.class, ARRAY_LIST);
      numbers.put(getPrivateArrayListClass(), ARRAY_LIST);
      numbers.put(getPrivateUnmodifiableListClass(), ARRAY_LIST);
      numbers.put(LinkedList.class, LINKED_LIST);
      numbers.put(getPrivateEmptyListClass(), EMPTY_LIST);
   }

   @Override
   public void writeObject(ObjectOutput output, List list) throws IOException {
      int number = numbers.get(list.getClass(), -1);
      output.writeByte(number);
      switch (number) {
         case ARRAY_LIST:
         case LINKED_LIST:
            MarshallUtil.marshallCollection(list, output);
            break;
      }
   }

   @Override
   public List readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      switch (magicNumber) {
         case ARRAY_LIST:
            return MarshallUtil.unmarshallCollection(input, ArrayList::new);
         case LINKED_LIST:
            return MarshallUtil.unmarshallCollection(input, s -> new LinkedList<>());
         case EMPTY_LIST:
            return Collections.emptyList();
         default:
            throw new IllegalStateException("Unknown List type: " + magicNumber);
      }
   }

   @Override
   public Integer getId() {
      return Ids.ARRAY_LIST;
   }

   @Override
   public Set<Class<? extends List>> getTypeClasses() {
      return Util.asSet(ArrayList.class, LinkedList.class,
            getPrivateArrayListClass(),
            getPrivateUnmodifiableListClass(),
            getPrivateEmptyListClass());
   }

   private static Class<List> getPrivateArrayListClass() {
      return getListClass("java.util.Arrays$ArrayList");
   }

   private static Class<List> getPrivateUnmodifiableListClass() {
      return getListClass("java.util.Collections$UnmodifiableRandomAccessList");
   }

   private static Class<List> getPrivateEmptyListClass() {
      return getListClass("java.util.Collections$EmptyList");
   }

   private static Class<List> getListClass(String className) {
      return Util.<List>loadClass(className, List.class.getClassLoader());
   }

}
