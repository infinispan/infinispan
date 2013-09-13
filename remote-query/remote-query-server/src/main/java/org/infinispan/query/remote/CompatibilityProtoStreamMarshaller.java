package org.infinispan.query.remote;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;

/**
 * A per EmbeddedCacheManager marshaller that can be used as compatibility mode marshaller.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class CompatibilityProtoStreamMarshaller extends BaseProtoStreamMarshaller {

   private EmbeddedCacheManager cacheManager;

   public CompatibilityProtoStreamMarshaller() {
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   public void setCacheManager(EmbeddedCacheManager cacheManager) {  //todo [anistor] this cannot work in xml config mode ..
      this.cacheManager = cacheManager;
   }

   @Override
   protected SerializationContext getSerializationContext() {
      if (cacheManager == null) {
         throw new IllegalStateException("cacheManager not set");
      }
      return ProtobufMetadataManager.getSerializationContext(cacheManager);
   }
}
