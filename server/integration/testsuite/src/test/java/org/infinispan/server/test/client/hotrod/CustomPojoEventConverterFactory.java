package org.infinispan.server.test.client.hotrod;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.server.test.client.hotrod.AbstractRemoteCacheIT.Person;

import java.io.Serializable;

@NamedFactory(name = "pojo-converter-factory")
public class CustomPojoEventConverterFactory implements CacheEventConverterFactory {
    @Override
    public CacheEventConverter<Integer, Person, CustomEvent> getConverter(final Object[] params) {
        return new DynamicCacheEventConverter(params);
    }

    static class DynamicCacheEventConverter implements CacheEventConverter<Integer, Person, CustomEvent>, Serializable {
        private final Object[] params;

        public DynamicCacheEventConverter(Object[] params) {
            this.params = params;
        }


        @Override
        public CustomEvent convert(Integer key, Person oldValue, Metadata oldMetadata,
              Person newValue, Metadata newMetadata, EventType eventType) {
            if (newValue == null || newValue.equals(params[0]))
                return new CustomEvent<Integer, Person>(key, null);

            return new CustomEvent<Integer, Person>(key, newValue);
        }
    }

}
