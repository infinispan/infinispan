package org.infinispan.query.remote.impl;

import java.security.PrivilegedAction;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;

/**
 * @author Dan Berindei
 * @since 10.0
 */
public class GetSerializationContextAction implements PrivilegedAction<SerializationContext> {
   private final EmbeddedCacheManager cacheManager;

   public GetSerializationContextAction(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public SerializationContext run() {
      return ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
   }
}
