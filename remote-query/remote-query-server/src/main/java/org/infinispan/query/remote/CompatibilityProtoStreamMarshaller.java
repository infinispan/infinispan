package org.infinispan.query.remote;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;

/**
 * A per EmbeddedCacheManager marshaller that can be used as compatibility mode marshaller. An instance cannot be shared
 * between multiple cache managers.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class CompatibilityProtoStreamMarshaller extends BaseProtoStreamMarshaller {

   private EmbeddedCacheManager cacheManager;

   public CompatibilityProtoStreamMarshaller() {
   }

   @Inject
   protected void injectDependencies(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   protected SerializationContext getSerializationContext() {
      if (cacheManager == null) {
         throw new IllegalStateException("cacheManager not set");
      }
      return ProtobufMetadataManager.getSerializationContextInternal(cacheManager);
   }
}
