package org.infinispan.server.test.client.hotrod;

import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.filter.NamedFactory;

import java.io.Serializable;

@NamedFactory(name = "filter-converter-factory")
public class FilterConverterFactory implements CacheEventFilterConverterFactory {

   @Override
   public CacheEventFilterConverter<Integer, String, CustomEvent> getFilterConverter(Object[] params) {
      return new FilterConverter(params);
   }

   static class FilterConverter extends AbstractCacheEventFilterConverter<Integer, String, CustomEvent>
      implements Serializable {
      private final Object[] params;

      public FilterConverter(Object[] params) {
         this.params = params;
      }

      @Override
      public CustomEvent filterAndConvert(Integer key, String oldValue, Metadata oldMetadata,
         String newValue, Metadata newMetadata, EventType eventType) {
         if (params[0].equals(key))
            return new CustomEvent(key, null);

         return new CustomEvent(key, newValue);
      }
   }
}
