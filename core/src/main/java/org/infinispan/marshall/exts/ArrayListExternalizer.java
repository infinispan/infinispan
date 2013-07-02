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
import java.util.ArrayList;
import java.util.Set;

/**
 * List externalizer dealing with ArrayList and LinkedList implementations.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class ArrayListExternalizer extends AbstractExternalizer<ArrayList> {

   @Override
   public void writeObject(ObjectOutput output, ArrayList list) throws IOException {
      MarshallUtil.marshallCollection(list, output);
   }

   @Override
   public ArrayList readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int size = UnsignedNumeric.readUnsignedInt(input);
      ArrayList<Object> l = new ArrayList<Object>(size);
      for (int i = 0; i < size; i++) l.add(input.readObject());
      return l;
   }

   @Override
   public Integer getId() {
      return Ids.ARRAY_LIST;
   }

   @Override
   public Set<Class<? extends ArrayList>> getTypeClasses() {
      return Util.<Class<? extends ArrayList>>asSet(ArrayList.class);
   }
}
