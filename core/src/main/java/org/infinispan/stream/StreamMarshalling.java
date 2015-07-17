package org.infinispan.stream;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Static factory class containing methods that will provide marshallable instances for very common use cases.
 * Every instance returned from the various static methods uses the Infinispan marshalling to reduce payload sizes
 * considerably and should be used whenever possible.
 */
public class StreamMarshalling {
   private StreamMarshalling() { }

   /**
    * Provides a predicate that returns true when the object is equal.
    * @param object the instance to test equality on
    * @return the predicate
    */
   public static Predicate<Object> equalityPredicate(Object object) {
      return new EqualityPredicate(object);
   }

   public static Predicate<Object> nonNullPredicate() {
      return NonNullPredicate.getInstance();
   }

   /**
    * Provides a function that returns the key of the entry when invoked.
    * @param <K> key type of the entry
    * @param <V> value type of the entry
    * @return a function that when applied to a given entry will return the key
    */
   public static <K, V> Function<Map.Entry<K, V>, K> entryToKeyFunction() {
      return EntryToKeyFunction.getInstance();
   }

   /**
    * Provides a function that returns the value of the entry when invoked.
    * @param <K> key type of the entry
    * @param <V> value type of the entry
    * @return a function that when applied to a given entry will return the value
    */
   public static <K, V> Function<Map.Entry<K, V>, V> entryToValueFunction() {
      return EntryToValueFunction.getInstance();
   }

   private static final class EqualityPredicate implements Predicate<Object> {
      private final Object object;

      private EqualityPredicate(Object object) {
         Objects.nonNull(object);
         this.object = object;
      }

      @Override
      public boolean test(Object t) {
         return object.equals(t);
      }
   }

   private static final class NonNullPredicate implements Predicate<Object> {
      private static final NonNullPredicate INSTANCE = new NonNullPredicate();

      public static NonNullPredicate getInstance() {
         return INSTANCE;
      }
      @Override
      public boolean test(Object t) {
         return t != null;
      }
   }

   private static final class EntryToKeyFunction<K, V> implements Function<Map.Entry<K, V>, K> {
      private static final EntryToKeyFunction<?, ?> FUNCTION = new EntryToKeyFunction<>();

      public static <K, V> EntryToKeyFunction<K, V> getInstance() {
         return (EntryToKeyFunction<K, V>) FUNCTION;
      }

      @Override
      public K apply(Map.Entry<K, V> kvEntry) {
         return kvEntry.getKey();
      }
   }

   private static final class EntryToValueFunction<K, V> implements Function<Map.Entry<K, V>, V> {
      private static final EntryToValueFunction<?, ?> FUNCTION = new EntryToValueFunction<>();

      public static <K, V> EntryToValueFunction<K, V> getInstance() {
         return (EntryToValueFunction<K, V>) FUNCTION;
      }

      @Override
      public V apply(Map.Entry<K, V> kvEntry) {
         return kvEntry.getValue();
      }
   }

   public static final class StreamMarshallingExternalizer implements AdvancedExternalizer<Object> {

      private static final int EQUALITY_PREDICATE = 0;
      private static final int ENTRY_KEY_FUNCTION = 1;
      private static final int ENTRY_VALUE_FUNCTION = 2;
      private static final int NON_NULL_PREDICATE = 3;

      private final IdentityIntMap<Class<? extends Object>> objects = new IdentityIntMap<>();

      public StreamMarshallingExternalizer() {
         objects.put(EqualityPredicate.class, EQUALITY_PREDICATE);
         objects.put(EntryToKeyFunction.class, ENTRY_KEY_FUNCTION);
         objects.put(EntryToValueFunction.class, ENTRY_VALUE_FUNCTION);
         objects.put(NonNullPredicate.class, NON_NULL_PREDICATE);
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return Util.<Class<? extends Object>>asSet(EqualityPredicate.class, EntryToKeyFunction.class,
                 EntryToValueFunction.class, NonNullPredicate.class);
      }

      @Override
      public Integer getId() {
         return Ids.STREAM_MARSHALLING;
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         int number = objects.get(object.getClass(), -1);
         output.writeByte(number);
         switch (number) {
            case EQUALITY_PREDICATE:
               output.writeObject(((EqualityPredicate) object).object);
               break;
         }
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int number = input.readUnsignedByte();
         switch (number) {
            case EQUALITY_PREDICATE:
               return new EqualityPredicate(input.readObject());
            case ENTRY_KEY_FUNCTION:
               return EntryToKeyFunction.getInstance();
            case ENTRY_VALUE_FUNCTION:
               return EntryToValueFunction.getInstance();
            case NON_NULL_PREDICATE:
               return NonNullPredicate.getInstance();
            default:
               throw new IllegalArgumentException("Found invalid number " + number);
         }
      }
   }
}
