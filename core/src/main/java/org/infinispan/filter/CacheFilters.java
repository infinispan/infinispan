package org.infinispan.filter;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.stream.StreamMarshalling;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Static factory class that contains utility methods that can be used for performing proper transformations from
 * {@link KeyValueFilter}, {@link Converter} & {@link KeyValueFilterConverter} to appropraite distributed stream
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
    * @param <C> convertered value type
    * @return function based on the converter
    */
   public static <K, V, C> Function<CacheEntry<K, V>, CacheEntry<K, C>> function(
           Converter<? super K, ? super V, C> converter) {
      return new ConverterAsCacheEntryFunction<>(converter);
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
      return stream.map(new FilterConverterAsCacheEntryFunction(filterConverter)).filter(
              StreamMarshalling.nonNullPredicate());
   }

   private static class KeyValueFilterAsPredicate<K, V> implements Predicate<CacheEntry<K, V>> {
      private final KeyValueFilter<? super K, ? super V> filter;

      public KeyValueFilterAsPredicate(KeyValueFilter<? super K, ? super V> filter) {
         Objects.nonNull(filter);
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

   private static class ConverterAsCacheEntryFunction<K, V, C> implements Function<CacheEntry<K, V>, CacheEntry<K, C>> {
      protected final Converter<? super K, ? super V, C> converter;

      protected InternalEntryFactory factory;

      public ConverterAsCacheEntryFunction(Converter<? super K, ? super V, C> converter) {
         Objects.nonNull(converter);
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

   private static class FilterConverterAsCacheEntryFunction<K, V, C> implements Function<CacheEntry<K, V>, CacheEntry<K, C>> {
      protected final KeyValueFilterConverter<? super K, ? super V, C> converter;

      protected InternalEntryFactory factory;

      public FilterConverterAsCacheEntryFunction(KeyValueFilterConverter<? super K, ? super V, C> converter) {
         Objects.nonNull(converter);
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
         C converted = converter.filterAndConvert(key, value, metadata);
         if (converted == null) {
            return null;
         }
         return factory.create(key, converted, metadata);
      }
   }

   public static final class CacheFiltersExternalizer implements AdvancedExternalizer<Object> {

      private static final int KEY_VALUE_FILTER_PREDICATE = 0;
      private static final int CONVERTER_FUNCTION = 1;
      private static final int FILTER_CONVERTER_FUNCTION = 2;

      private final IdentityIntMap<Class<? extends Object>> objects = new IdentityIntMap<>();

      public CacheFiltersExternalizer() {
         objects.put(KeyValueFilterAsPredicate.class, KEY_VALUE_FILTER_PREDICATE);
         objects.put(ConverterAsCacheEntryFunction.class, CONVERTER_FUNCTION);
         objects.put(FilterConverterAsCacheEntryFunction.class, FILTER_CONVERTER_FUNCTION);
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return Util.<Class<? extends Object>>asSet(KeyValueFilterAsPredicate.class, ConverterAsCacheEntryFunction.class,
                 FilterConverterAsCacheEntryFunction.class);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_FILTERS;
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         int number = objects.get(object.getClass(), -1);
         output.writeByte(number);
         switch (number) {
            case KEY_VALUE_FILTER_PREDICATE:
               output.writeObject(((KeyValueFilterAsPredicate) object).filter);
               break;
            case CONVERTER_FUNCTION:
               output.writeObject(((ConverterAsCacheEntryFunction) object).converter);
               break;
            case FILTER_CONVERTER_FUNCTION:
               output.writeObject(((FilterConverterAsCacheEntryFunction) object).converter);
               break;
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
            default:
               throw new IllegalArgumentException("Found invalid number " + number);
         }
      }
   }
}
