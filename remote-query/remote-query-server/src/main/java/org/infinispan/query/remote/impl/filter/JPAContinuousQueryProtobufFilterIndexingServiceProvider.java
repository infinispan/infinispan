package org.infinispan.query.remote.impl.filter;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.continuous.impl.JPAContinuousQueryFilterIndexingServiceProvider;
import org.infinispan.query.remote.client.ContinuousQueryResult;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.kohsuke.MetaInfServices;

import java.io.IOException;


/**
 * @author anistor@redhat.com
 * @since 8.1
 */
@MetaInfServices(FilterIndexingServiceProvider.class)
@SuppressWarnings("unused")
public final class JPAContinuousQueryProtobufFilterIndexingServiceProvider extends JPAContinuousQueryFilterIndexingServiceProvider {

   private SerializationContext serCtx;

   private boolean isCompatMode;

   @Inject
   protected void injectDependencies(EmbeddedCacheManager cacheManager, Cache c) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager);
      isCompatMode = c.getCacheConfiguration().compatibility().enabled();
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

         boolean isJoining = Boolean.TRUE.equals(eventType);
         Object result = new ContinuousQueryResult(isJoining, (byte[]) key, (byte[]) instance, projection);

         if (!isCompatMode) {
            result = ProtobufUtil.toWrappedByteArray(serCtx, result);
         }

         return result;
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
