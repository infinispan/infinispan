package org.infinispan.server.functional.extensions.filters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.server.functional.extensions.entities.Entities;

@NamedFactory(name = "filter-converter-factory")
public class FilterConverterFactory implements CacheEventFilterConverterFactory {

   @Override
   public CacheEventFilterConverter<Integer, String, Entities.CustomEvent<Integer>> getFilterConverter(Object[] params) {
      return new FilterConverter(params);
   }

   public static class FilterConverter extends AbstractCacheEventFilterConverter<Integer, String, Entities.CustomEvent<Integer>> {
      private final Object[] params;

      @ProtoField(number = 1, defaultValue = "0")
      int count;

      FilterConverter(Object[] params) {
         this.params = params;
         this.count = 0;
      }

      @ProtoFactory
      FilterConverter(List<WrappedMessage> wrappedParams, int count) {
         this.params = wrappedParams == null ? null : wrappedParams.stream().map(WrappedMessage::getValue).toArray();
         this.count = count;
      }

      @ProtoField(number = 2, collectionImplementation = ArrayList.class)
      List<WrappedMessage> getWrappedParams() {
         return Arrays.stream(params).map(WrappedMessage::new).collect(Collectors.toList());
      }

      @Override
      public Entities.CustomEvent<Integer> filterAndConvert(Integer key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
         count++;
         if (params[0].equals(key)) return new Entities.CustomEvent<>(key, null, count);

         return new Entities.CustomEvent<>(key, newValue, count);
      }
   }
}
