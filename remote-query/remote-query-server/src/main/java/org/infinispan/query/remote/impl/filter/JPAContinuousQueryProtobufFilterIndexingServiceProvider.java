package org.infinispan.query.remote.impl.filter;

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

   @Inject
   protected void injectDependencies(EmbeddedCacheManager cacheManager) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager);
   }

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == JPAContinuousQueryProtobufCacheEventFilterConverter.class;
   }

   @Override
   protected Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
      boolean isJoining = Boolean.TRUE.equals(eventType);
      try {
         return ProtobufUtil.toByteArray(serCtx, new ContinuousQueryResult(isJoining, (byte[]) key, (byte[]) instance, projection));
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
}
