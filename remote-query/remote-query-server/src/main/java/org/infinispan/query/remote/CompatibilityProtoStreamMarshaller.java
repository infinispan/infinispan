package org.infinispan.query.remote;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;

/**
 * A per {@link EmbeddedCacheManager} marshaller that should be used as compatibility mode marshaller in server. An
 * instance cannot be shared between multiple cache managers.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class CompatibilityProtoStreamMarshaller extends BaseProtoStreamMarshaller {

   @Inject protected EmbeddedCacheManager cacheManager;

   private SerializationContext serCtx;

   @Start
   void start() {
      serCtx = SecurityActions.getSerializationContext(cacheManager);
   }

   @Override
   protected SerializationContext getSerializationContext() {
      return serCtx;
   }
}
