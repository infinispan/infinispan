package org.infinispan.marshall.exts;

import net.jcip.annotations.Immutable;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.util.ReadOnlySegmentAwareSet;
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
   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(2);

   public SetExternalizer() {
      numbers.put(HashSet.class, HASH_SET);
      numbers.put(TreeSet.class, TREE_SET);
      numbers.put(ReadOnlySegmentAwareSet.class, HASH_SET);
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
      return Ids.JDK_SETS;
   }

   @Override
   public Set<Class<? extends Set>> getTypeClasses() {
      return Util.<Class<? extends Set>>asSet(HashSet.class, TreeSet.class,
         ReadOnlySegmentAwareSet.class);
   }

}
