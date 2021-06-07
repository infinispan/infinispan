package org.infinispan.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.infinispan.CacheStream;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Static factory class that contains utility methods that can be used for performing proper transformations from
 * {@link KeyValueFilter}, {@link Converter} and {@link KeyValueFilterConverter} to appropriate distributed stream
 * instances.
 */
public final class CacheFilters {
   private CacheFilters() { }

   /**
    * Creates a new {@link Predicate} using the provided key value filter as a basis for the operation.  This is useful
    * for when using {@link Stream#filter(Predicate)} method on distributed streams.  The key,
    * value and metadata are all used to determine if the predicate returns true or not.
    * @param filter the filter to utilize
    * @param <K> key type
    * @param <V> value type
    * @return predicate based on the filter
    */
   public static <K, V> Predicate<CacheEntry<K, V>> predicate(KeyValueFilter<? super K, ? super V> filter) {
      return new KeyValueFilterAsPredicate<>(filter);
   }

   /**
    * Creates a new {@link Function} using the provided converter as a basis for the operation.  This is useful
    * for when using {@link Stream#map(Function)} method on distributed streams.  The key,
    * value and metadata are all used to determine the converted value.
    * @param converter the converter to utilize
    * @param <K> key type
    * @param <V> value type
    * @param <C> converted value type
    * @return function based on the converter
    */
   public static <K, V, C> Function<CacheEntry<K, V>, CacheEntry<K, C>> function(
           Converter<? super K, ? super V, C> converter) {
      return new ConverterAsCacheEntryFunction<>(converter);
   }

   /**
    * Creates a new {@link Function} using the provided filter convert. The {@link KeyValueFilterConverter#filterAndConvert(Object, Object, Metadata)}
    * is invoked for every passed in CacheEntry. When a null value is returned from the filter converter instead of
    * a null {@link CacheEntry} being returned, we instead return the {@link NullCacheEntry} as a sign of it. This
    * allows this {@link Function} to be used in cases when a null value cannot be returned, such as reactive streams.
    * <p>
    * The {@link #notNullCacheEntryPredicate()} can be used to filter these values if needed.
    * @param filterConverter the filter converter to utilize
    * @param <K> key type
    * @param <V> value type
    * @param <C> converted value type
    * @return function based on the filter converter
    */
   public static <K, V, C> Function<CacheEntry<K, V>, CacheEntry<K, C>> converterToFunction(KeyValueFilterConverter<? super K, ? super V, C> filterConverter) {
      return new FilterConverterAsCacheEntryFunction<>(filterConverter);
   }

   /**
    * Provides a predicate that can be used to filter out null cache entry objects as well as placeholder "null" entries
    * of {@link NullCacheEntry} that can be returned from methods such as {@link #converterToFunction(KeyValueFilterConverter)}.
    * @param <K> entry key type
    * @param <V> entry value type
    * @return a predicate to filter null entries
    */
   public static <K, V> Predicate<CacheEntry<K, V>> notNullCacheEntryPredicate() {
      return NotNullCacheEntryPredicate.SINGLETON;
   }

   public static <K, V, C> CacheStream<K> filterAndConvertToKey(CacheStream<CacheEntry<K, V>> stream,
         KeyValueFilterConverter<? super K, ? super V, C> filterConverter) {
      // Have to use flatMap instead of map/filter as reactive streams spec doesn't allow null values
      return stream.flatMap(new FilterConverterAsKeyFunction(filterConverter));
   }

   public static <K, V, C> CacheStream<C> filterAndConvertToValue(CacheStream<CacheEntry<K, V>> stream,
         KeyValueFilterConverter<? super K, ? super V, C> filterConverter) {
      // Have to use flatMap instead of map/filter as reactive streams spec doesn't allow null values
      return stream.flatMap(new FilterConverterAsValueFunction(filterConverter));
   }

   /**
    * Adds needed intermediate operations to the provided stream, returning a possibly new stream as a result of the
    * operations.  This method keeps the contract of filter and conversion being performed in only 1 call as the
    * {@link KeyValueFilterConverter} was designed to do.  The key,
    * value and metadata are all used to determine whether the value is returned and the converted value.
    * @param stream stream to perform the operations on
    * @param filterConverter converter to apply
    * @param <K>
    * @param <V>
    * @param <C>
    * @return
    */
   public static <K, V, C> Stream<CacheEntry<K, C>> filterAndConvert(Stream<CacheEntry<K, V>> stream,
           KeyValueFilterConverter<? super K, ? super V, C> filterConverter) {
      return stream.map(converterToFunction(filterConverter))
            .filter(notNullCacheEntryPredicate());
   }

   public static <K, V, C> CacheStream<CacheEntry<K, C>> filterAndConvert(CacheStream<CacheEntry<K, V>> stream,
            KeyValueFilterConverter<? super K, ? super V, C> filterConverter) {
      return stream.map(converterToFunction(filterConverter))
            .filter(notNullCacheEntryPredicate());
   }

   @Scope(Scopes.NONE)
   static class NotNullCacheEntryPredicate<K, V> implements Predicate<CacheEntry<K, V>> {
      private static final NotNullCacheEntryPredicate SINGLETON = new NotNullCacheEntryPredicate();

      @Override
      public boolean test(CacheEntry<K, V> kvCacheEntry) {
         return kvCacheEntry != null && kvCacheEntry != NullCacheEntry.getInstance();
      }
   }

   @Scope(Scopes.NONE)
   static final class FilterConverterAsCacheEntryFunction<K, V, C> implements Function<CacheEntry<K, V>, CacheEntry<K, C>> {
      private final KeyValueFilterConverter<? super K, ? super V, C> filterConverter;

      private InternalEntryFactory factory;

      FilterConverterAsCacheEntryFunction(KeyValueFilterConverter<? super K, ? super V, C> filterConverter) {
         this.filterConverter = Objects.requireNonNull(filterConverter);
      }

      @Inject
      public void inject(InternalEntryFactory factory, ComponentRegistry registry) {
         this.factory = factory;
         registry.wireDependencies(filterConverter);
      }

      @Override
      public CacheEntry<K, C> apply(CacheEntry<K, V> kvCacheEntry) {
         K key = kvCacheEntry.getKey();
         V value = kvCacheEntry.getValue();
         Metadata metadata = kvCacheEntry.getMetadata();
         C converted = filterConverter.filterAndConvert(key, value, metadata);
         if (converted == null) {
            return NullCacheEntry.getInstance();
         } else if (value == converted) {
            return (CacheEntry<K, C>) kvCacheEntry;
         }
         return factory.create(key, converted, metadata);
      }
   }

   @Scope(Scopes.NONE)
   static final class KeyValueFilterAsPredicate<K, V> implements Predicate<CacheEntry<K, V>> {
      private final KeyValueFilter<? super K, ? super V> filter;

      public KeyValueFilterAsPredicate(KeyValueFilter<? super K, ? super V> filter) {
         Objects.requireNonNull(filter);
         this.filter = filter;
      }

      @Override
      public boolean test(CacheEntry<K, V> kvCacheEntry) {
         return filter.accept(kvCacheEntry.getKey(), kvCacheEntry.getValue(), kvCacheEntry.getMetadata());
      }

      @Inject
      public void inject(ComponentRegistry registry) {
         registry.wireDependencies(filter);
      }
   }

   @Scope(Scopes.NONE)
   static final class ConverterAsCacheEntryFunction<K, V, C> implements Function<CacheEntry<K, V>, CacheEntry<K, C>> {
      private final Converter<? super K, ? super V, C> converter;

      private InternalEntryFactory factory;

      public ConverterAsCacheEntryFunction(Converter<? super K, ? super V, C> converter) {
         Objects.requireNonNull(converter);
         this.converter = converter;
      }

      @Inject
      public void inject(InternalEntryFactory factory, ComponentRegistry registry) {
         this.factory = factory;
         registry.wireDependencies(converter);
      }

      @Override
      public CacheEntry<K, C> apply(CacheEntry<K, V> kvCacheEntry) {
         K key = kvCacheEntry.getKey();
         V value = kvCacheEntry.getValue();
         Metadata metadata = kvCacheEntry.getMetadata();
         C converted = converter.convert(key, value, metadata);
         if (value == converted) {
            return (CacheEntry<K, C>) kvCacheEntry;
         }
         return factory.create(key, converted, metadata);
      }
   }

   @Scope(Scopes.NONE)
   static final class FilterConverterAsKeyFunction<K, V> implements Function<CacheEntry<K, V>, Stream<K>> {
      private final KeyValueFilterConverter<? super K, ? super V, ?> converter;

      public FilterConverterAsKeyFunction(KeyValueFilterConverter<? super K, ? super V, ? super K> converter) {
         Objects.requireNonNull(converter);
         this.converter = converter;
      }

      @Inject
      public void inject(ComponentRegistry registry) {
         registry.wireDependencies(converter);
      }

      @Override
      public Stream<K> apply(CacheEntry<K, V> entry) {
         Object converted = converter.filterAndConvert(entry.getKey(), entry.getValue(), entry.getMetadata());
         if (converted == null) {
            return Stream.empty();
         }
         return Stream.of(entry.getKey());
      }
   }

   @Scope(Scopes.NONE)
   static final class FilterConverterAsValueFunction<K, V, C> implements Function<CacheEntry<K, V>, Stream<C>> {
      private final KeyValueFilterConverter<? super K, ? super V, C> converter;

      public FilterConverterAsValueFunction(KeyValueFilterConverter<? super K, ? super V, C> converter) {
         Objects.requireNonNull(converter);
         this.converter = converter;
      }

      @Inject
      public void inject(ComponentRegistry registry) {
         registry.wireDependencies(converter);
      }

      @Override
      public Stream<C> apply(CacheEntry<K, V> entry) {
         C converted = converter.filterAndConvert(entry.getKey(), entry.getValue(), entry.getMetadata());
         if (converted == null) {
            return Stream.empty();
         }
         return Stream.of(converted);
      }
   }

   public static final class CacheFiltersExternalizer implements AdvancedExternalizer<Object> {

      private static final int KEY_VALUE_FILTER_PREDICATE = 0;
      private static final int CONVERTER_FUNCTION = 1;
      private static final int FILTER_CONVERTER_FUNCTION = 2;
      private static final int FILTER_CONVERTER_VALUE_FUNCTION = 3;
      private static final int NOT_NULL_CACHE_ENTRY_PREDICATE = 4;
      private static final int FILTER_CONVERTER_KEY_FUNCTION = 5;

      private final Map<Class<?>, Integer> objects = new HashMap<>();

      public CacheFiltersExternalizer() {
         objects.put(KeyValueFilterAsPredicate.class, KEY_VALUE_FILTER_PREDICATE);
         objects.put(ConverterAsCacheEntryFunction.class, CONVERTER_FUNCTION);
         objects.put(FilterConverterAsCacheEntryFunction.class, FILTER_CONVERTER_FUNCTION);
         objects.put(FilterConverterAsKeyFunction.class, FILTER_CONVERTER_KEY_FUNCTION);
         objects.put(FilterConverterAsValueFunction.class, FILTER_CONVERTER_VALUE_FUNCTION);
         objects.put(NotNullCacheEntryPredicate.class, NOT_NULL_CACHE_ENTRY_PREDICATE);
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return Util.asSet(KeyValueFilterAsPredicate.class, ConverterAsCacheEntryFunction.class,
                 FilterConverterAsCacheEntryFunction.class, FilterConverterAsKeyFunction.class,
                 FilterConverterAsValueFunction.class, NotNullCacheEntryPredicate.class);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_FILTERS;
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         int number = objects.getOrDefault(object.getClass(), -1);
         output.writeByte(number);
         switch (number) {
            case KEY_VALUE_FILTER_PREDICATE:
               output.writeObject(((KeyValueFilterAsPredicate) object).filter);
               break;
            case CONVERTER_FUNCTION:
               output.writeObject(((ConverterAsCacheEntryFunction) object).converter);
               break;
            case FILTER_CONVERTER_FUNCTION:
               output.writeObject(((FilterConverterAsCacheEntryFunction) object).filterConverter);
               break;
            case FILTER_CONVERTER_KEY_FUNCTION:
               output.writeObject(((FilterConverterAsKeyFunction) object).converter);
               break;
            case FILTER_CONVERTER_VALUE_FUNCTION:
               output.writeObject(((FilterConverterAsValueFunction) object).converter);
               break;
            case NOT_NULL_CACHE_ENTRY_PREDICATE:
               break;
            default:
               throw new IllegalArgumentException("Type " + number + " is not supported!");
         }
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int number = input.readUnsignedByte();
         switch (number) {
            case KEY_VALUE_FILTER_PREDICATE:
               return new KeyValueFilterAsPredicate((KeyValueFilter) input.readObject());
            case CONVERTER_FUNCTION:
               return new ConverterAsCacheEntryFunction((Converter) input.readObject());
            case FILTER_CONVERTER_FUNCTION:
               return new FilterConverterAsCacheEntryFunction((KeyValueFilterConverter) input.readObject());
            case FILTER_CONVERTER_KEY_FUNCTION:
               return new FilterConverterAsKeyFunction((KeyValueFilterConverter) input.readObject());
            case FILTER_CONVERTER_VALUE_FUNCTION:
               return new FilterConverterAsValueFunction((KeyValueFilterConverter) input.readObject());
            case NOT_NULL_CACHE_ENTRY_PREDICATE:
               return NotNullCacheEntryPredicate.SINGLETON;
            default:
               throw new IllegalArgumentException("Found invalid number " + number);
         }
      }
   }
}
