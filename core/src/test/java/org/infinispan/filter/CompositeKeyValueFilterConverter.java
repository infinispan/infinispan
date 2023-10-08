package org.infinispan.filter;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

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
public class CompositeKeyValueFilterConverter<K, V, C> implements KeyValueFilterConverter<K, V, C> {
   private final KeyValueFilter<? super K, ? super V> filter;
   private final Converter<? super K, ? super V, ? extends C> converter;

   public CompositeKeyValueFilterConverter(KeyValueFilter<? super K, ? super V> filter,
                                           Converter<? super K, ? super V, ? extends C> converter) {
      this.filter = filter;
      this.converter = converter;
   }

   @ProtoFactory
   CompositeKeyValueFilterConverter(MarshallableObject<KeyValueFilter<? super K, ? super V>> filter,
                                    MarshallableObject<Converter<? super K, ? super V, ? extends C>> converter) {
      this.filter = MarshallableObject.unwrap(filter);
      this.converter = MarshallableObject.unwrap(converter);
   }

   @ProtoField(number = 1)
   MarshallableObject<KeyValueFilter<? super K, ? super V>> getFilter() {
      return MarshallableObject.create(filter);
   }

   @ProtoField(number = 2)
   MarshallableObject<Converter<? super K, ? super V, ? extends C>> getConverter() {
      return MarshallableObject.create(converter);
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
