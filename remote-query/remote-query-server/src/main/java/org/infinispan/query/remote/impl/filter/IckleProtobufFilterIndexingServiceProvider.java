package org.infinispan.query.remote.impl.filter;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.query.core.impl.eventfilter.IckleFilterIndexingServiceProvider;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.impl.RemoteQueryManager;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@MetaInfServices(FilterIndexingServiceProvider.class)
public final class IckleProtobufFilterIndexingServiceProvider extends IckleFilterIndexingServiceProvider {

   private RemoteQueryManager remoteQueryManager;

   @Inject Cache cache;

   private RemoteQueryManager getRemoteQueryManager() {
      if (remoteQueryManager == null) {
         remoteQueryManager = ComponentRegistry.componentOf(cache, RemoteQueryManager.class);
      }
      return remoteQueryManager;
   }

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == IckleProtobufCacheEventFilterConverter.class;
   }

   @Override
   protected Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
      Object filterResult = new FilterResult(instance, projection, sortProjection);
      return getRemoteQueryManager().encodeFilterResult(filterResult);
   }
}
