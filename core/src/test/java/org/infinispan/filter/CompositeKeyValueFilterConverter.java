package org.infinispan.filter;

import org.infinispan.metadata.Metadata;

import java.io.Serializable;

/**
 * Allows to composite a KeyValueFilter and a Converter together to form a KeyValueFilterConverter.  There are no
 * performance gains by doing this though since the
 * {@link org.infinispan.filter.CompositeKeyValueFilterConverter#filterAndConvert(Object, Object, org.infinispan.metadata.Metadata)}
 * just composes of calling the filter and then converter as needed completely invalidating it's usage.  This is more
 * for testing where performance is not of a concern.
 *
 * @author wburns
 * @since 7.0
 */
public class CompositeKeyValueFilterConverter<K, V, C> implements KeyValueFilterConverter<K, V, C>, Serializable {
   private final KeyValueFilter<? super K, ? super V> filter;
   private final Converter<? super K, ? super V, ? extends C> converter;

   public CompositeKeyValueFilterConverter(KeyValueFilter<? super K, ? super V> filter,
                                           Converter<? super K, ? super V, ? extends C> converter) {
      this.filter = filter;
      this.converter = converter;
   }

   @Override
   public C filterAndConvert(K key, V value, Metadata metadata) {
      if (accept(key, value, metadata)) {
         return convert(key, value, metadata);
      }
      else {
         return null;
      }
   }

   @Override
   public C convert(K key, V value, Metadata metadata) {
      return converter.convert(key, value, metadata);
   }

   @Override
   public boolean accept(K key, V value, Metadata metadata) {
      return filter.accept(key, value, metadata);
   }
}
