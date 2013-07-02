package org.infinispan.marshall.exts;

import net.jcip.annotations.Immutable;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.Set;

/**
 * LinkedListExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class LinkedListExternalizer extends AbstractExternalizer<LinkedList> {

   @Override
   public void writeObject(ObjectOutput output, LinkedList list) throws IOException {
      MarshallUtil.marshallCollection(list, output);
   }

   @Override
   public LinkedList readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int size = UnsignedNumeric.readUnsignedInt(input);
      LinkedList<Object> l = new LinkedList();
      for (int i = 0; i < size; i++) l.add(input.readObject());
      return l;
   }

   @Override
   public Integer getId() {
      return Ids.LINKED_LIST;
   }

   @Override
   public Set<Class<? extends LinkedList>> getTypeClasses() {
      return Util.<Class<? extends LinkedList>>asSet(LinkedList.class);
   }
}
