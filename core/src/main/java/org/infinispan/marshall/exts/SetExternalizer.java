package org.infinispan.marshall.exts;

import net.jcip.annotations.Immutable;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Set externalizer for all set implementations, i.e. HashSet and TreeSet
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class SetExternalizer extends AbstractExternalizer<Set> {
   private static final int HASH_SET = 0;
   private static final int TREE_SET = 1;
   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>(2);

   public SetExternalizer() {
      numbers.put(HashSet.class, HASH_SET);
      numbers.put(TreeSet.class, TREE_SET);
   }

   @Override
   public void writeObject(ObjectOutput output, Set set) throws IOException {
      int number = numbers.get(set.getClass(), -1);
      output.writeByte(number);
      if (number == TREE_SET)
         output.writeObject(((TreeSet) set).comparator());

      MarshallUtil.marshallCollection(set, output);
   }

   @Override
   public Set readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      Set<Object> subject = null;
      switch (magicNumber) {
         case HASH_SET:
            subject = new HashSet();
            break;
         case TREE_SET:
            Comparator comparator = (Comparator) input.readObject();
            subject = new TreeSet(comparator);
            break;
      }
      int size = UnsignedNumeric.readUnsignedInt(input);
      for (int i = 0; i < size; i++) subject.add(input.readObject());
      return subject;
   }

   @Override
   public Integer getId() {
      return Ids.JDK_SETS;
   }

   @Override
   public Set<Class<? extends Set>> getTypeClasses() {
      return Util.<Class<? extends Set>>asSet(HashSet.class, TreeSet.class);
   }

}