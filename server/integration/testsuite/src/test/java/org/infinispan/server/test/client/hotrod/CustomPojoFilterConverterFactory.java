package org.infinispan.server.test.client.hotrod;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.server.test.client.hotrod.AbstractRemoteCacheIT.Id;
import org.infinispan.server.test.client.hotrod.AbstractRemoteCacheIT.Person;

import java.io.Serializable;

@NamedFactory(name = "pojo-filter-converter-factory")
public class CustomPojoFilterConverterFactory implements CacheEventFilterConverterFactory {

   @Override
   public CacheEventFilterConverter<Id, Person, CustomEvent> getFilterConverter(Object[] params) {
      return new FilterConverter(params);
   }

   static class FilterConverter extends AbstractCacheEventFilterConverter<Id, Person, CustomEvent>
      implements Serializable {
      private final Object[] params;

      public FilterConverter(Object[] params) {
         this.params = params;
      }

      @Override
      public CustomEvent filterAndConvert(Id key, Person oldValue, Metadata oldMetadata,
            Person newValue, Metadata newMetadata, EventType eventType) {
         if (params[0].equals(key))
            return new CustomEvent<Id, Person>(key, null);

         return new CustomEvent<Id, Person>(key, newValue);
      }
   }
}
