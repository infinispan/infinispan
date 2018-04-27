package org.infinispan.tools.store.migrator.marshaller.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.jboss.marshalling.util.IdentityIntMap;

import net.jcip.annotations.Immutable;

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
   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(2);

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
      switch (magicNumber) {
         case HASH_SET:
            return MarshallUtil.unmarshallCollection(input, s -> new HashSet<>());
         case TREE_SET:
            Comparator<Object> comparator = (Comparator<Object>) input.readObject();
            return MarshallUtil.unmarshallCollection(input, s -> new TreeSet<>(comparator));
         default:
            throw new IllegalStateException("Unknown Set type: " + magicNumber);
      }
   }

   @Override
   public Integer getId() {
      return LegacyIds.JDK_SETS;
   }

   @Override
   public Set<Class<? extends Set>> getTypeClasses() {
      return Util.asSet(HashSet.class, TreeSet.class);
   }
}
