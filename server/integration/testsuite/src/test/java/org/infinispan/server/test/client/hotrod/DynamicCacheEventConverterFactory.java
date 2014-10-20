package org.infinispan.server.test.client.hotrod;

import org.infinispan.notifications.cachelistener.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;

import java.io.Serializable;

@NamedFactory(name = "dynamic-converter-factory")
public class DynamicCacheEventConverterFactory implements CacheEventConverterFactory {
    @Override
    public CacheEventConverter<Integer, String, CustomEvent> getConverter(final Object[] params) {
        return new DynamicCacheEventConverter(params);
    }

    static class DynamicCacheEventConverter implements CacheEventConverter<Integer, String, CustomEvent>, Serializable {
        private final Object[] params;

        public DynamicCacheEventConverter(Object[] params) {
            this.params = params;
        }


        @Override
        public CustomEvent convert(Integer key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
            if (params[0].equals(key))
                return new CustomEvent(key, null);

            return new CustomEvent(key, newValue);
        }
    }

}
