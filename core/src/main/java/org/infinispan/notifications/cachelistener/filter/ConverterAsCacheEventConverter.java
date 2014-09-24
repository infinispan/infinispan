package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.filter.Converter;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Set;

/**
 * CacheEventConverter that implements it's conversion solely on the use of the provided Converter
 *
 * @author wburns
 * @since 7.0
 */
public class ConverterAsCacheEventConverter<K, V, C> implements CacheEventConverter<K, V, C>, Serializable {
   private final Converter<K, V, C> converter;

   public ConverterAsCacheEventConverter(Converter<K, V, C> converter) {
      this.converter = converter;
   }

   @Override
   public C convert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return converter.convert(key, newValue, newMetadata);
   }

   public static class Externalizer extends AbstractExternalizer<ConverterAsCacheEventConverter> {
      @Override
      public Set<Class<? extends ConverterAsCacheEventConverter>> getTypeClasses() {
         return Util.<Class<? extends ConverterAsCacheEventConverter>>asSet(ConverterAsCacheEventConverter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ConverterAsCacheEventConverter object) throws IOException {
         output.writeObject(object.converter);
      }

      @Override
      public ConverterAsCacheEventConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ConverterAsCacheEventConverter((Converter)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.CONVERTER_AS_CACHE_EVENT_CONVERTER;
      }
   }
}
