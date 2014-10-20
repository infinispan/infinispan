package org.infinispan.server.test.client.hotrod;

import org.infinispan.notifications.cachelistener.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;

import java.io.Serializable;

@NamedFactory(name = "static-filter-factory")
public class StaticCacheEventFilterFactory implements CacheEventFilterFactory {
    @Override
    public CacheEventFilter<Integer, String> getFilter(final Object[] params) {
        return new StaticCacheEventFilter();
    }

    static class StaticCacheEventFilter implements CacheEventFilter<Integer, String>, Serializable {
        final Integer staticKey = 2;

        @Override
        public boolean accept(Integer key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
           return staticKey.equals(key);
        }
    }

}
