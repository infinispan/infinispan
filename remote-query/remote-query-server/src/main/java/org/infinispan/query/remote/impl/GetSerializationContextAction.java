package org.infinispan.query.remote.impl;

import java.util.function.Supplier;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;

/**
 * @author Dan Berindei
 * @since 10.0
 */
public final class GetSerializationContextAction implements Supplier<SerializationContext> {

   private final EmbeddedCacheManager cacheManager;

   public GetSerializationContextAction(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public SerializationContext get() {
      return ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
   }
}
