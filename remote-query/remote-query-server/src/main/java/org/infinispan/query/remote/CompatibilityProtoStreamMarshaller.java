package org.infinispan.query.remote;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;

/**
 * A per {@link EmbeddedCacheManager} marshaller that should be used as compatibility mode marshaller (see {@link
 * org.infinispan.interceptors.compat.TypeConverterInterceptor}) in server. An instance cannot be shared between
 * multiple cache managers.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class CompatibilityProtoStreamMarshaller extends BaseProtoStreamMarshaller {

   protected EmbeddedCacheManager cacheManager;

   public CompatibilityProtoStreamMarshaller() {
   }

   @Inject
   @SuppressWarnings("unused")
   protected void injectDependencies(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   protected SerializationContext getSerializationContext() {
      if (cacheManager == null) {
         throw new IllegalStateException("cacheManager not set");
      }
      return ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager);
   }
}
