package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.util.ReadOnlySegmentAwareSet;
import org.infinispan.marshall.core.Ids;
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
   private static final int SINGLETON_SET = 2;
   private static final int SYNCHRONIZED_SET = 3;

   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(5);

   public SetExternalizer() {
      numbers.put(HashSet.class, HASH_SET);
      numbers.put(TreeSet.class, TREE_SET);
      numbers.put(ReadOnlySegmentAwareSet.class, HASH_SET);
      numbers.put(getPrivateSingletonSetClass(), SINGLETON_SET);
      numbers.put(getPrivateSynchronizedSetClass(), SYNCHRONIZED_SET);
      numbers.put(getPrivateUnmodifiableSetClass(), HASH_SET);
   }

   @Override
   public void writeObject(ObjectOutput output, Set set) throws IOException {
      int number = numbers.get(set.getClass(), -1);
      output.writeByte(number);
      switch (number) {
         case HASH_SET:
         case SYNCHRONIZED_SET:
            MarshallUtil.marshallCollection(set, output);
            break;
         case TREE_SET:
            output.writeObject(((TreeSet) set).comparator());
            MarshallUtil.marshallCollection(set, output);
            break;
         case SINGLETON_SET:
            Object singleton = set.iterator().next();
            output.writeObject(singleton);
            break;
      }
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
         case SINGLETON_SET:
            return Collections.singleton(input.readObject());
         case SYNCHRONIZED_SET:
            return Collections.synchronizedSet(
                  MarshallUtil.unmarshallCollection(input, s -> new HashSet<>()));
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
         ReadOnlySegmentAwareSet.class, getPrivateSingletonSetClass(),
         getPrivateSynchronizedSetClass(), getPrivateUnmodifiableSetClass());
   }

   public static Class<Set> getPrivateSingletonSetClass() {
      return getSetClass("java.util.Collections$SingletonSet");
   }

   public static Class<Set> getPrivateSynchronizedSetClass() {
      return getSetClass("java.util.Collections$SynchronizedSet");
   }

   private static Class<Set> getPrivateUnmodifiableSetClass() {
      return getSetClass("java.util.Collections$UnmodifiableSet");
   }

   private static Class<Set> getSetClass(String className) {
      return Util.<Set>loadClass(className, Set.class.getClassLoader());
   }

}
