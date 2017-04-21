package org.infinispan.query.remote.impl.filter;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.embedded.impl.IckleFilterIndexingServiceProvider;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@MetaInfServices(FilterIndexingServiceProvider.class)
@SuppressWarnings("unused")
public final class IckleProtobufFilterIndexingServiceProvider extends IckleFilterIndexingServiceProvider {

   private SerializationContext serCtx;

   private boolean isCompatMode;

   @Inject
   protected void injectDependencies(Cache cache) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cache.getCacheManager());
      isCompatMode = cache.getCacheConfiguration().compatibility().enabled();
   }

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == IckleProtobufCacheEventFilterConverter.class;
   }

   @Override
   protected Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
      Object result = new FilterResult(instance, projection, sortProjection);

      if (!isCompatMode) {
         try {
            result = ProtobufUtil.toWrappedByteArray(serCtx, result);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }

      return result;
   }
}
