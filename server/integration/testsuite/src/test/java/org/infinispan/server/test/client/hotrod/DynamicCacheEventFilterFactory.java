package org.infinispan.server.test.client.hotrod;

import org.infinispan.notifications.cachelistener.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;

import java.io.Serializable;

@NamedFactory(name = "dynamic-filter-factory")
public class DynamicCacheEventFilterFactory implements CacheEventFilterFactory {
    @Override
    public CacheEventFilter<Integer, String> getFilter(final Object[] params) {
        return new DynamicCacheEventFilter(params);
    }

    static class DynamicCacheEventFilter implements CacheEventFilter<Integer, String>, Serializable {
        private final Object[] params;

        public DynamicCacheEventFilter(Object[] params) {
            this.params = params;
        }

        @Override
        public boolean accept(Integer key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
           return params[0].equals(key); // dynamic
        }
    }
}
