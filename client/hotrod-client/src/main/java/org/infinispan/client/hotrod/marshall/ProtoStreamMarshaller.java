package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class ProtoStreamMarshaller extends BaseProtoStreamMarshaller {

   private final SerializationContext serializationContext = ProtobufUtil.newSerializationContext(new Configuration.Builder().build());

   public ProtoStreamMarshaller() {
   }

   @Override
   public SerializationContext getSerializationContext() {
      return serializationContext;
   }

   public static SerializationContext getSerializationContext(RemoteCacheManager remoteCacheManager) {
      Marshaller marshaller = remoteCacheManager.getMarshaller();
      if (marshaller instanceof ProtoStreamMarshaller) {
         return ((ProtoStreamMarshaller) marshaller).getSerializationContext();
      }
      throw new HotRodClientException("The cache manager must be configured with a ProtoStreamMarshaller");
   }
}
