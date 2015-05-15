package org.infinispan.server.test.client.hotrod;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.server.test.client.hotrod.AbstractRemoteCacheIT.Person;

import java.io.Serializable;

@NamedFactory(name = "pojo-filter-factory")
public class CustomPojoEventFilterFactory implements CacheEventFilterFactory {

   @Override
   public CacheEventFilter<Integer, Person> getFilter(Object[] params) {
      return new CustomPojoCacheEventFilter(params);
   }

   private static class CustomPojoCacheEventFilter implements CacheEventFilter<Integer, Person>, Serializable {
      private final Object[] params;

      private CustomPojoCacheEventFilter(Object[] params) {
         this.params = params;
      }

      @Override
      public boolean accept(Integer key, Person oldValue, Metadata oldMetadata,
            Person newValue, Metadata newMetadata, EventType eventType) {
         return newValue == null
               ? oldValue.getName().equals(params[0])
               : newValue.getName().equals(params[0]);
      }
   }
}
