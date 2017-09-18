package org.infinispan.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.commons.util.Util;
import org.jboss.marshalling.util.IdentityIntMap;

/**
 * Externalizer to be used for IntSet implementations
 * @author wburns
 * @since 9.0
 */
public class IntSetExternalizer extends AbstractExternalizer<IntSet> {
   private static final int RANGESET = 0;
   private static final int SMALLINTSET = 1;

   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>(2);

   public IntSetExternalizer() {
      numbers.put(RangeSet.class, RANGESET);
      numbers.put(org.infinispan.commons.util.RangeSet.class, RANGESET);
      numbers.put(SmallIntSet.class, SMALLINTSET);
   }

   @Override
   public Integer getId() {
      return Ids.INT_SET;
   }

   @Override
   public Set<Class<? extends IntSet>> getTypeClasses() {
      return Util.asSet(SmallIntSet.class, RangeSet.class, org.infinispan.commons.util.RangeSet.class);
   }

   @Override
   public void writeObject(ObjectOutput output, IntSet object) throws IOException {
      int number = numbers.get(object.getClass(), -1);
      output.write(number);
      switch (number) {
         case RANGESET:
            UnsignedNumeric.writeUnsignedInt(output, object.size());
            break;
         case SMALLINTSET:
            SmallIntSet.writeTo(output, (SmallIntSet) object);
            break;
         default:
            throw new UnsupportedOperationException("Unsupported number: " + number);
      }
   }

   @Override
   public IntSet readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      switch (magicNumber) {
         case RANGESET:
            return new org.infinispan.commons.util.RangeSet(UnsignedNumeric.readUnsignedInt(input));
         case SMALLINTSET:
            return SmallIntSet.readFrom(input);
         default:
            throw new UnsupportedOperationException("Unsupported number: " + magicNumber);
      }
   }
}
