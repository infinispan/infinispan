package org.infinispan.commons.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;

/**
 * IntSets externalization mechanism
 * @author wburns
 * @since 9.3
 */
public class IntSetsExternalization {
   private IntSetsExternalization() { }

   private static final int RANGESET = 0;
   private static final int SMALLINTSET = 1;
   private static final int EMPTYSET = 2;
   private static final int SINGLETONSET = 3;
   private static final int CONCURRENTSET = 4;

   private final static Map<Class<?>, Integer> numbers = new HashMap<>(5);

   static {
      numbers.put(RangeSet.class, RANGESET);
      numbers.put(org.infinispan.commons.util.RangeSet.class, RANGESET);
      numbers.put(SmallIntSet.class, SMALLINTSET);
      numbers.put(EmptyIntSet.class, EMPTYSET);
      numbers.put(SingletonIntSet.class, SINGLETONSET);
      numbers.put(ConcurrentSmallIntSet.class, CONCURRENTSET);
   }

   public static void writeTo(ObjectOutput output, IntSet intSet) throws IOException {
      int number = numbers.getOrDefault(intSet.getClass(), -1);
      output.write(number);
      switch (number) {
         case RANGESET:
            UnsignedNumeric.writeUnsignedInt(output, intSet.size());
            break;
         case SMALLINTSET:
            SmallIntSet.writeTo(output, (SmallIntSet) intSet);
            break;
         case EMPTYSET:
            break;
         case SINGLETONSET:
            UnsignedNumeric.writeUnsignedInt(output, ((SingletonIntSet) intSet).value);
            break;
         case CONCURRENTSET:
            ConcurrentSmallIntSet.writeTo(output, (ConcurrentSmallIntSet) intSet);
            break;
         default:
            throw new UnsupportedOperationException("Unsupported number: " + number);
      }
   }

   public static IntSet readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      switch (magicNumber) {
         case RANGESET:
            return new org.infinispan.commons.util.RangeSet(UnsignedNumeric.readUnsignedInt(input));
         case SMALLINTSET:
            return SmallIntSet.readFrom(input);
         case EMPTYSET:
            return EmptyIntSet.getInstance();
         case SINGLETONSET:
            return new SingletonIntSet(UnsignedNumeric.readUnsignedInt(input));
         case CONCURRENTSET:
            return ConcurrentSmallIntSet.readFrom(input);
         default:
            throw new UnsupportedOperationException("Unsupported number: " + magicNumber);
      }
   }

   public static Set<Class<? extends IntSet>> getTypeClasses() {
      return Util.asSet(SmallIntSet.class, RangeSet.class, org.infinispan.commons.util.RangeSet.class, EmptyIntSet.class,
            SingletonIntSet.class, ConcurrentSmallIntSet.class);
   }
}
