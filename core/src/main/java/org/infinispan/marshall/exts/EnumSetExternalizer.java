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
import java.util.Set;

/**
 * {@link EnumSet} externalizer.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Immutable
public class EnumSetExternalizer extends AbstractExternalizer<Set> {

   private static final int ENUM_SET = 0;
   private static final int REGULAR_ENUM_SET = 1;
   private static final int JUMBO_ENUM_SET = 2;

   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>(3);

   public EnumSetExternalizer() {
      numbers.put(EnumSet.class, ENUM_SET);
      numbers.put(getRegularEnumSetClass(), REGULAR_ENUM_SET);
      numbers.put(getJumboEnumSetClass(), JUMBO_ENUM_SET);
   }

   @Override
   public void writeObject(ObjectOutput output, Set set) throws IOException {
      int number = numbers.get(set.getClass(), -1);
      output.writeByte(number);
      MarshallUtil.marshallCollection(set, output);
   }

   @Override
   public Set readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      AbstractSet<Enum> enumSet = null;
      int size = UnsignedNumeric.readUnsignedInt(input);
      for (int i = 0; i < size; i++) {
         switch (magicNumber) {
            case ENUM_SET:
            case REGULAR_ENUM_SET:
            case JUMBO_ENUM_SET:
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
      return Util.<Class<? extends Set>>asSet(
            EnumSet.class, getRegularEnumSetClass(), getJumboEnumSetClass());
   }

   private Class<EnumSet> getJumboEnumSetClass() {
      return getEnumSetClass("java.util.JumboEnumSet");
   }

   private Class<EnumSet> getRegularEnumSetClass() {
      return getEnumSetClass("java.util.RegularEnumSet");
   }

   private Class<EnumSet> getEnumSetClass(String className) {
      return Util.loadClass(className, EnumSet.class.getClassLoader());
   }

}
