package org.infinispan.server.functional.extensions.filters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.server.functional.extensions.entities.Entities;

@NamedFactory(name = "dynamic-converter-factory")
public class DynamicConverterFactory<K> implements CacheEventConverterFactory {
   @Override
   public CacheEventConverter<K, String, Entities.CustomEvent<K>> getConverter(final Object[] params) {
      return new DynamicConverter<>(params);
   }

   public static class DynamicConverter<K> implements CacheEventConverter<K, String, Entities.CustomEvent<K>>, Serializable {
      private final Object[] params;

      public DynamicConverter(Object[] params) {
         this.params = params;
      }

      @ProtoFactory
      DynamicConverter(ArrayList<WrappedMessage> wrappedParams) {
         this.params = wrappedParams == null ? null : wrappedParams.stream().map(WrappedMessage::getValue).toArray();
      }

      @ProtoField(number = 1, collectionImplementation = ArrayList.class)
      List<WrappedMessage> getWrappedParams() {
         return Arrays.stream(params).map(WrappedMessage::new).collect(Collectors.toList());
      }

      @Override
      public Entities.CustomEvent<K> convert(K key, String previousValue, Metadata previousMetadata, String value,
                                             Metadata metadata, EventType eventType) {
         if (params[0].equals(key))
            return new Entities.CustomEvent<>(key, null, 0);

         return new Entities.CustomEvent<>(key, value, 0);
      }
   }
}
