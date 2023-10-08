package org.infinispan.stream;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

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
      return new EqualityPredicate(MarshallableObject.create(object));
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

   /**
    * Same as {@link Function#identity()} except that this instance is also able to be marshalled by Infinispan.
    * @param <T> any type
    * @return function that just returns the provided value
    */
   public static <T> Function<T, T> identity() {
      return IdentityFunction.getInstance();
   }

   /**
    * Provides a function that given a key will return the {@link CacheEntry} that maps to this
    * key. This function only works when used with a {@link org.infinispan.CacheStream} returned
    * from the desired {@link Cache}. The entry will be read from the <b>Cache</b> of which the
    * <b>CacheStream</b> was created from.
    * @param <K> the key type
    * @param <V> the expected value type of the entry
    * @return a function that when applied returns the entry for the given key
    */
   public static <K, V> Function<K, CacheEntry<K, V>> keyToEntryFunction() {
      return new KeyToEntryFunction<>();
   }

   @ProtoTypeId(ProtoStreamTypeIds.EQUALITY_PREDICATE)
   public static final class EqualityPredicate implements Predicate<Object> {

      @ProtoField(1)
      final MarshallableObject<Object> object;

      @ProtoFactory
      EqualityPredicate(MarshallableObject<Object> object) {
         this.object = object;
      }

      @Override
      public boolean test(Object t) {
         return object.get().equals(t);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.NON_NULL_PREDICATE)
   public static final class NonNullPredicate implements Predicate<Object> {
      private static final NonNullPredicate INSTANCE = new NonNullPredicate();

      @ProtoFactory
      public static NonNullPredicate getInstance() {
         return INSTANCE;
      }

      @Override
      public boolean test(Object t) {
         return t != null;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.ALWAYS_TRUE_PREDICATE)
   public static final class AlwaysTruePredicate implements Predicate<Object> {
      private static final AlwaysTruePredicate INSTANCE = new AlwaysTruePredicate();

      @ProtoFactory
      public static AlwaysTruePredicate getInstance() {
         return INSTANCE;
      }

      @Override
      public boolean test(Object t) {
         return true;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.ENTRY_KEY_FUNCTION)
   public static final class EntryToKeyFunction<K, V> implements Function<Map.Entry<K, V>, K> {
      private static final EntryToKeyFunction<?, ?> FUNCTION = new EntryToKeyFunction<>();

      @ProtoFactory
      public static <K, V> EntryToKeyFunction<K, V> getInstance() {
         return (EntryToKeyFunction<K, V>) FUNCTION;
      }

      @Override
      public K apply(Map.Entry<K, V> kvEntry) {
         return kvEntry.getKey();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.ENTRY_VALUE_FUNCTION)
   public static final class EntryToValueFunction<K, V> implements Function<Map.Entry<K, V>, V> {
      private static final EntryToValueFunction<?, ?> FUNCTION = new EntryToValueFunction<>();

      @ProtoFactory
      public static <K, V> EntryToValueFunction<K, V> getInstance() {
         return (EntryToValueFunction<K, V>) FUNCTION;
      }

      @Override
      public V apply(Map.Entry<K, V> kvEntry) {
         return kvEntry.getValue();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.KEY_ENTRY_FUNCTION)
   @Scope(Scopes.NONE)
   static final class KeyToEntryFunction<K, V> implements Function<K, CacheEntry<K, V>> {
      @Inject AdvancedCache<K, V> advancedCache;

      @Override
      public CacheEntry<K, V> apply(K k) {
         return advancedCache.getCacheEntry(k);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.IDENTITY_FUNCTION)
   public static final class IdentityFunction<T> implements Function<T, T> {
      private static final IdentityFunction<?> FUNCTION = new IdentityFunction<>();

      @ProtoFactory
      public static <T> IdentityFunction<T> getInstance() {
         return (IdentityFunction<T>) FUNCTION;
      }

      @Override
      public T apply(T t) {
         return t;
      }
   }
}
