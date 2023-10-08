package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.AbstractSet;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.util.Util;

import net.jcip.annotations.Immutable;

@Immutable
public class EnumSetExternalizer extends AbstractMigratorExternalizer<Set> {

   private static final int UNKNOWN_ENUM_SET = 0;
   private static final int ENUM_SET = 1;
   private static final int REGULAR_ENUM_SET = 2;
   private static final int JUMBO_ENUM_SET = 3;
   private static final int MINI_ENUM_SET = 4; // IBM class
   private static final int HUGE_ENUM_SET = 5; // IBM class

   public EnumSetExternalizer() {
      super(getClasses(), Ids.ENUM_SET_ID);
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

   public static Set<Class<? extends Set>> getClasses() {
      Set<Class<? extends Set>> set = new HashSet<Class<? extends Set>>();
      set.add(EnumSet.class);
      addEnumSetType(getRegularEnumSetClass(), set);
      addEnumSetType(getJumboEnumSetClass(), set);
      addEnumSetType(getMiniEnumSetClass(), set);
      addEnumSetType(getHugeEnumSetClass(), set);
      return set;
   }

   private static void addEnumSetType(Class<? extends Set> clazz, Set<Class<? extends Set>> typeSet) {
      if (clazz != null)
         typeSet.add(clazz);
   }

   private static Class<EnumSet> getJumboEnumSetClass() {
      return getEnumSetClass("java.util.JumboEnumSet");
   }

   private static Class<EnumSet> getRegularEnumSetClass() {
      return getEnumSetClass("java.util.RegularEnumSet");
   }

   private static Class<EnumSet> getMiniEnumSetClass() {
      return getEnumSetClass("java.util.MiniEnumSet");
   }

   private static Class<EnumSet> getHugeEnumSetClass() {
      return getEnumSetClass("java.util.HugeEnumSet");
   }

   private static Class<EnumSet> getEnumSetClass(String className) {
      try {
         return Util.loadClassStrict(className, EnumSet.class.getClassLoader());
      } catch (ClassNotFoundException e) {
         return null; // Ignore
      }
   }
}
