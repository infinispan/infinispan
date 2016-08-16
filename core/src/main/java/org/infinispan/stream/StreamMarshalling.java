package org.infinispan.stream;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

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

   /**
    * Predicate that returns true if the object passed to it is not null.
    * @return the predicate
    */
   public static Predicate<Object> nonNullPredicate() {
      return NonNullPredicate.getInstance();
   }

   /**
    * Predicate taht always returns true irrespective of the value provided
    * @return the predicate
    */
   public static Predicate<Object> alwaysTruePredicate() {
      return AlwaysTruePredicate.getInstance();
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

   private static final class AlwaysTruePredicate implements Predicate<Object> {
      private static final AlwaysTruePredicate INSTANCE = new AlwaysTruePredicate();

      public static AlwaysTruePredicate getInstance() {
         return INSTANCE;
      }
      @Override
      public boolean test(Object t) {
         return true;
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
      enum ExternalizerId {
         EQUALITY_PREDICATE(EqualityPredicate.class),
         ENTRY_KEY_FUNCTION(EntryToKeyFunction.class),
         ENTRY_VALUE_FUNCTION(EntryToValueFunction.class),
         NON_NULL_PREDICATE(NonNullPredicate.class),
         ALWAYS_TRUE_PREDICATE(AlwaysTruePredicate.class);

         private final Class<? extends Object> marshalledClass;

         ExternalizerId(Class<? extends Object> marshalledClass) {
            this.marshalledClass = marshalledClass;
         }
      }

      private final Map<Class<? extends Object>, ExternalizerId> objects = new HashMap<>();

      public StreamMarshallingExternalizer() {
         for (ExternalizerId id : ExternalizerId.values()) {
            objects.put(id.marshalledClass, id);
         }
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return Util.<Class<? extends Object>>asSet(EqualityPredicate.class, EntryToKeyFunction.class,
                 EntryToValueFunction.class, NonNullPredicate.class, AlwaysTruePredicate.class);
      }

      @Override
      public Integer getId() {
         return Ids.STREAM_MARSHALLING;
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         ExternalizerId id = objects.get(object.getClass());
         if (id == null) {
            throw new IllegalArgumentException("Unsupported class " + object.getClass() + " was provided!");
         }
         output.writeByte(id.ordinal());
         switch (id) {
            case EQUALITY_PREDICATE:
               output.writeObject(((EqualityPredicate) object).object);
               break;
         }
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int number = input.readUnsignedByte();
         ExternalizerId[] ids = ExternalizerId.values();
         if (number < 0 || number >= ids.length) {
            throw new IllegalArgumentException("Found invalid number " + number);
         }
         ExternalizerId id = ids[number];
         switch (id) {
            case EQUALITY_PREDICATE:
               return new EqualityPredicate(input.readObject());
            case ENTRY_KEY_FUNCTION:
               return EntryToKeyFunction.getInstance();
            case ENTRY_VALUE_FUNCTION:
               return EntryToValueFunction.getInstance();
            case NON_NULL_PREDICATE:
               return NonNullPredicate.getInstance();
            case ALWAYS_TRUE_PREDICATE:
               return AlwaysTruePredicate.getInstance();
            default:
               throw new IllegalArgumentException("ExternalizerId not supported: " + id);
         }
      }
   }
}
