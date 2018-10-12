package org.infinispan.query.remote.impl.filter;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.query.continuous.impl.IckleContinuousQueryFilterIndexingServiceProvider;
import org.infinispan.query.remote.client.ContinuousQueryResult;
import org.infinispan.query.remote.impl.RemoteQueryManager;
import org.kohsuke.MetaInfServices;


/**
 * @author anistor@redhat.com
 * @since 8.1
 */
@MetaInfServices(FilterIndexingServiceProvider.class)
@SuppressWarnings("unused")
public final class IckleContinuousQueryProtobufFilterIndexingServiceProvider extends IckleContinuousQueryFilterIndexingServiceProvider {

   private RemoteQueryManager remoteQueryManager;

   @Inject private Cache cache;

   public IckleContinuousQueryProtobufFilterIndexingServiceProvider() {
      super(ContinuousQueryResult.ResultType.JOINING, ContinuousQueryResult.ResultType.UPDATED, ContinuousQueryResult.ResultType.LEAVING);
   }

   private RemoteQueryManager getRemoteQueryManager() {
      if (remoteQueryManager == null) {
         remoteQueryManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RemoteQueryManager.class);
      }
      return remoteQueryManager;
   }

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == IckleContinuousQueryProtobufCacheEventFilterConverter.class;
   }

   @Override
   protected Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
      key = getRemoteQueryManager().convertKey(key, MediaType.APPLICATION_PROTOSTREAM);

      if (instance != null) {
         instance = getRemoteQueryManager().convertValue(instance, MediaType.APPLICATION_PROTOSTREAM);
      }

      ContinuousQueryResult result = new ContinuousQueryResult((ContinuousQueryResult.ResultType) eventType, (byte[]) key, (byte[]) instance, projection);
      return getRemoteQueryManager().encodeFilterResult(result);
   }
}
