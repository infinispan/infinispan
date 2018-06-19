package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;

/**
 * A marshaller that uses Protocol Buffers.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class ProtoStreamMarshaller extends BaseProtoStreamMarshaller {

   private final SerializationContext serializationContext = ProtobufUtil.newSerializationContext();

   public ProtoStreamMarshaller() {
   }

   @Override
   public SerializationContext getSerializationContext() {
      return serializationContext;
   }

   /**
    * Obtains the {@link SerializationContext} associated with the given remote cache manager.
    *
    * @param remoteCacheManager the remote cache manager (must not be {@code null})
    * @return the associated {@link SerializationContext}
    * @throws HotRodClientException if the cache manager is not configured to use a {@link ProtoStreamMarshaller}
    */
   public static SerializationContext getSerializationContext(RemoteCacheManager remoteCacheManager) {
      Marshaller marshaller = remoteCacheManager.getMarshaller();
      if (marshaller instanceof ProtoStreamMarshaller) {
         return ((ProtoStreamMarshaller) marshaller).getSerializationContext();
      }
      throw new HotRodClientException("The cache manager must be configured with a ProtoStreamMarshaller");
   }
}
