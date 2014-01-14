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
import java.util.AbstractSet;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link EnumSet} externalizer.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Immutable
public class EnumSetExternalizer extends AbstractExternalizer<Set> {

   private static final int UNKNOWN_ENUM_SET = 0;
   private static final int ENUM_SET = 1;
   private static final int REGULAR_ENUM_SET = 2;
   private static final int JUMBO_ENUM_SET = 3;
   private static final int MINI_ENUM_SET = 4; // IBM class
   private static final int HUGE_ENUM_SET = 5; // IBM class

   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>(3);

   public EnumSetExternalizer() {
      numbers.put(EnumSet.class, ENUM_SET);
      addEnumSetClass(getRegularEnumSetClass(), REGULAR_ENUM_SET);
      addEnumSetClass(getJumboEnumSetClass(), JUMBO_ENUM_SET);
      addEnumSetClass(getMiniEnumSetClass(), MINI_ENUM_SET);
      addEnumSetClass(getHugeEnumSetClass(), HUGE_ENUM_SET);
   }

   private void addEnumSetClass(Class<EnumSet> clazz, int index) {
      if (clazz != null)
         numbers.put(clazz, index);
   }

   @Override
   public void writeObject(ObjectOutput output, Set set) throws IOException {
      int number = numbers.get(set.getClass(), UNKNOWN_ENUM_SET);
      if (number == UNKNOWN_ENUM_SET) {
         // Fallback on standard object write
         output.writeObject(set);
      } else {
         output.writeByte(number);
         MarshallUtil.marshallCollection(set, output);
      }
   }

   @Override
   public Set readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      if (magicNumber == UNKNOWN_ENUM_SET)
         return (Set) input.readObject();

      AbstractSet<Enum> enumSet = null;
      int size = UnsignedNumeric.readUnsignedInt(input);
      for (int i = 0; i < size; i++) {
         switch (magicNumber) {
            case ENUM_SET:
            case REGULAR_ENUM_SET:
            case JUMBO_ENUM_SET:
            case MINI_ENUM_SET:
            case HUGE_ENUM_SET:
               if (i == 0)
                  enumSet = EnumSet.of((Enum) input.readObject());
               else
                  enumSet.add((Enum) input.readObject());
               break;
         }
      }

      return enumSet;
   }

   @Override
   public Integer getId() {
      return Ids.ENUM_SET_ID;
   }

   @Override
   public Set<Class<? extends Set>> getTypeClasses() {
      Set<Class<? extends Set>> set = new HashSet<Class<? extends Set>>();
      set.add(EnumSet.class);
      addEnumSetType(getRegularEnumSetClass(), set);
      addEnumSetType(getJumboEnumSetClass(), set);
      addEnumSetType(getMiniEnumSetClass(), set);
      addEnumSetType(getHugeEnumSetClass(), set);
      return set;
   }

   private void addEnumSetType(Class<? extends Set> clazz, Set<Class<? extends Set>> typeSet) {
      if (clazz != null)
         typeSet.add(clazz);
   }

   private Class<EnumSet> getJumboEnumSetClass() {
      return getEnumSetClass("java.util.JumboEnumSet");
   }

   private Class<EnumSet> getRegularEnumSetClass() {
      return getEnumSetClass("java.util.RegularEnumSet");
   }

   private Class<EnumSet> getMiniEnumSetClass() {
      return getEnumSetClass("java.util.MiniEnumSet");
   }

   private Class<EnumSet> getHugeEnumSetClass() {
      return getEnumSetClass("java.util.HugeEnumSet");
   }

   private Class<EnumSet> getEnumSetClass(String className) {
      try {
         return Util.loadClassStrict(className, EnumSet.class.getClassLoader());
      } catch (ClassNotFoundException e) {
         return null; // Ignore
      }
   }

}
