package org.infinispan.server.test.client.hotrod;

import org.infinispan.notifications.cachelistener.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;

import java.io.Serializable;

@NamedFactory(name = "static-converter-factory")
public class StaticCacheEventConverterFactory implements CacheEventConverterFactory {
    @Override
    public CacheEventConverter<Integer, String, CustomEvent> getConverter(Object[] params) {
        return new StaticCacheEventConverter();
    }

    static class StaticCacheEventConverter implements CacheEventConverter<Integer, String, CustomEvent>, Serializable {
        @Override
        public CustomEvent convert(Integer key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
            return new CustomEvent(key, newValue);
        }
    }
}
