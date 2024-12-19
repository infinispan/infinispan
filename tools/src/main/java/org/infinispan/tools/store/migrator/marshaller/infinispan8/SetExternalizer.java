package org.infinispan.tools.store.migrator.marshaller.infinispan8;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.tools.store.migrator.marshaller.common.AbstractMigratorExternalizer;

import net.jcip.annotations.Immutable;

/**
 * Set externalizer for all set implementations, i.e. HashSet and TreeSet
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
class SetExternalizer extends AbstractMigratorExternalizer<Set> {
   private static final int HASH_SET = 0;
   private static final int TREE_SET = 1;

   public SetExternalizer() {
      super(Set.of(HashSet.class, TreeSet.class), ExternalizerTable.JDK_SETS);
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
}
