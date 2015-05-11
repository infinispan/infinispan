package org.infinispan.query.remote.filter;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.embedded.impl.JPAFilterIndexingServiceProvider;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.FilterResult;
import org.kohsuke.MetaInfServices;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@MetaInfServices
public final class JPAProtobufFilterIndexingServiceProvider extends JPAFilterIndexingServiceProvider {

   private SerializationContext serCtx;

   @Inject
   protected void injectDependencies(EmbeddedCacheManager cacheManager) {
      serCtx = ProtobufMetadataManager.getSerializationContextInternal(cacheManager);
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
