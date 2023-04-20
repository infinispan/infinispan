package org.infinispan.server.functional.extensions.filters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@NamedFactory(name = "dynamic-filter-factory")
public class DynamicCacheEventFilterFactory implements CacheEventFilterFactory {
   @Override
   public CacheEventFilter<Integer, String> getFilter(Object[] params) {
      return new DynamicCacheEventFilter(params);
   }

   public static class DynamicCacheEventFilter implements CacheEventFilter<Integer, String>, Serializable {
      private final Object[] params;

      public DynamicCacheEventFilter(Object[] params) {
         this.params = params;
      }

      @ProtoFactory
      DynamicCacheEventFilter(ArrayList<WrappedMessage> wrappedParams) {
         this.params = wrappedParams == null ? null : wrappedParams.stream().map(WrappedMessage::getValue).toArray();
      }

      @ProtoField(number = 1, collectionImplementation = ArrayList.class)
      List<WrappedMessage> getWrappedParams() {
         return Arrays.stream(params).map(WrappedMessage::new).collect(Collectors.toList());
      }

      @Override
      public boolean accept(Integer key, String previousValue, Metadata previousMetadata, String value,
                            Metadata metadata, EventType eventType) {
         return params[0].equals(key); // dynamic
      }
   }
}
