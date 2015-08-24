package org.infinispan.query.remote.impl.filter;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.embedded.impl.JPAFilterIndexingServiceProvider;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.kohsuke.MetaInfServices;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@MetaInfServices(FilterIndexingServiceProvider.class)
public final class JPAProtobufFilterIndexingServiceProvider extends JPAFilterIndexingServiceProvider {

   private SerializationContext serCtx;

   @Inject
   protected void injectDependencies(EmbeddedCacheManager cacheManager) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager);
   }

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == JPAProtobufCacheEventFilterConverter.class;
   }

   @Override
   protected Object makeFilterResult(Object instance, Object[] projection, Comparable[] sortProjection) {
      try {
         return ProtobufUtil.toWrappedByteArray(serCtx, new FilterResult(instance, projection, sortProjection));
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
}
