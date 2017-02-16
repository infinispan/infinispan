package org.infinispan.query.remote.impl.filter;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.continuous.impl.JPAContinuousQueryFilterIndexingServiceProvider;
import org.infinispan.query.remote.client.ContinuousQueryResult;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.kohsuke.MetaInfServices;


/**
 * @author anistor@redhat.com
 * @since 8.1
 */
@MetaInfServices(FilterIndexingServiceProvider.class)
@SuppressWarnings("unused")
public final class JPAContinuousQueryProtobufFilterIndexingServiceProvider extends JPAContinuousQueryFilterIndexingServiceProvider {

   private SerializationContext serCtx;

   private boolean isCompatMode;

   public JPAContinuousQueryProtobufFilterIndexingServiceProvider() {
      super(ContinuousQueryResult.ResultType.JOINING, ContinuousQueryResult.ResultType.UPDATED, ContinuousQueryResult.ResultType.LEAVING);
   }

   @Inject
   protected void injectDependencies(Cache cache) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cache.getCacheManager());
      isCompatMode = cache.getCacheConfiguration().compatibility().enabled();
   }

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == JPAContinuousQueryProtobufCacheEventFilterConverter.class;
   }

   @Override
   protected Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
      try {
         if (isCompatMode) {
            key = ProtobufUtil.toWrappedByteArray(serCtx, key);
            if (instance != null) {
               instance = ProtobufUtil.toWrappedByteArray(serCtx, instance);
            }
         }

         Object result = new ContinuousQueryResult((ContinuousQueryResult.ResultType) eventType, (byte[]) key, (byte[]) instance, projection);

         if (!isCompatMode) {
            result = ProtobufUtil.toWrappedByteArray(serCtx, result);
         }

         return result;
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
