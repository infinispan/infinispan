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
    * A convenience method to return the {@link SerializationContext} associated with the {@link ProtoStreamMarshaller}
    * configured on the provided {@link RemoteCacheManager}.
    *
    * @return the associated {@link SerializationContext}
    * @throws HotRodClientException if the cache manager is not started or is not configured to use a {@link ProtoStreamMarshaller}
    */
   public static SerializationContext getSerializationContext(RemoteCacheManager remoteCacheManager) {
      Marshaller marshaller = remoteCacheManager.getMarshaller();
      if (marshaller instanceof ProtoStreamMarshaller) {
         return ((ProtoStreamMarshaller) marshaller).getSerializationContext();
      }

      if (marshaller == null) {
         throw new HotRodClientException("The cache manager must be configured with a ProtoStreamMarshaller and must be started before attempting to retrieve the ProtoStream SerializationContext");
      }

      throw new HotRodClientException("The cache manager is not configured with a ProtoStreamMarshaller");
   }
}
